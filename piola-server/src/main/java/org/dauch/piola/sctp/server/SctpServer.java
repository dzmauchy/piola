package org.dauch.piola.sctp.server;

/*-
 * #%L
 * piola-server
 * %%
 * Copyright (C) 2024 dauch
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpMultiChannel;
import org.dauch.piola.api.*;
import org.dauch.piola.api.response.Response;
import org.dauch.piola.sctp.SctpUtils;
import org.dauch.piola.server.AbstractServer;

import java.io.*;
import java.net.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.stream.Stream;

import static com.sun.nio.sctp.MessageInfo.createOutgoing;
import static java.lang.System.Logger.Level.*;

public final class SctpServer extends AbstractServer<SctpRq, SctpRs> {

  private final SctpMultiChannel channel;

  public SctpServer(SctpServerConfig config) {
    super(config, SctpRq[]::new, SctpRs[]::new);
    try {
      channel = $("channel", SctpMultiChannel.open());
      SctpUtils.configure(channel, config);
      channel.bind(config.address(), config.backlog());
      if (addresses().noneMatch(a -> a.getAddress() != null && a.getAddress().isLoopbackAddress())) {
        channel.bindAddress(Inet4Address.getByAddress(new byte[]{127, 0, 0, 1}));
      }
      startThreads();
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to start server " + id));
    }
  }

  @Override
  public Stream<InetSocketAddress> addresses() {
    try {
      return channel.getAllLocalAddresses().stream().map(InetSocketAddress.class::cast);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public InetSocketAddress address(InetAddress address) {
    return new InetSocketAddress(address, getPort());
  }

  @Override
  public InetSocketAddress address(String host) {
    return new InetSocketAddress(host, getPort());
  }

  @Override
  public int getPort() {
    try {
      return ((InetSocketAddress) channel.getAllLocalAddresses().iterator().next()).getPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void mainLoop() {
    while (true) {
      var buf = readBuffers.get();
      try {
        var msg = channel.receive(buf, null, null);
        if (!running) {
          throw new InterruptedException("Not running");
        }
        doInMainLoop(buf, msg);
      } catch (ClosedChannelException | InterruptedException _) {
        readBuffers.release(buf);
        logger.log(INFO, "Closed");
        break;
      } catch (Throwable e) {
        readBuffers.release(buf);
        logger.log(ERROR, "Unexpected exception", e);
        unexpectedErrors.increment();
      }
    }
    logger.log(INFO, "Main loop finished");
  }

  private void doInMainLoop(ByteBuffer buf, MessageInfo msg) throws Exception {
    receivedSize.add(msg.bytes());
    receivedRequests.increment();
    if (!msg.isComplete()) {
      incompleteRequests.increment();
      try {
        while (true) {
          var m = channel.receive(buf, null, null);
          checkIncomplete(msg, m);
          receivedSize.add(m.bytes());
          if (m.isComplete()) {
            break;
          } else if (!buf.hasRemaining()) {
            closeAssociation(msg);
            closeAssociation(m);
            throw new BufferOverflowException();
          }
        }
      } catch (Throwable e) {
        readBuffers.release(buf);
        throw e;
      }
    }
    try {
      read(msg, buf);
      validRequests.increment();
    } catch (Throwable e) {
      readBuffers.release(buf);
      logger.log(ERROR, () -> "Unknown exception " + msg, e);
      brokenRequests.increment();
      closeAssociation(msg);
    }
  }

  private void checkIncomplete(MessageInfo msg, MessageInfo m) throws Exception {
    try {
      if (m.streamNumber() != msg.streamNumber())
        throw new StreamCorruptedException("Stream number mismatch: " + m);
      if (m.payloadProtocolID() != msg.payloadProtocolID())
        throw new StreamCorruptedException("Payload protocol ID mismatch: " + m);
      if (m.association().associationID() != msg.association().associationID())
        throw new StreamCorruptedException("Association mismatch: " + m);
    } catch (StreamCorruptedException e) {
      closeAssociation(msg);
      closeAssociation(m);
      throw e;
    }
  }

  private void read(MessageInfo msg, ByteBuffer buffer) throws Throwable {
    var context = new SerializationContext();
    var id = buffer.flip().getLong();
    var req = RequestFactory.request(buffer, context);
    if (req.hasPayload()) {
      requests.put(new SctpRq(id, msg, req, buffer, context));
    } else {
      requests.put(new SctpRq(id, msg, req, null, context));
      readBuffers.release(buffer);
    }
  }

  @Override
  protected void reject(SctpRq sctpRq) {
    closeAssociation(sctpRq.meta());
  }

  @Override
  protected void writeResponse(SctpRq rq, ByteBuffer payload, Response r) throws Exception {
    var msg = createOutgoing(rq.meta().association(), rq.meta().address(), rq.meta().streamNumber());
    var buf = writeBuffers.get();
    try {
      ResponseFactory.write(r, buf.putInt(id).putLong(rq.id()));
      rq.context().write(buf);
      if (payload != null) {
        buf.put(payload);
      }
      var count = channel.send(buf.flip(), msg);
      sentSize.add(count);
      sentMessages.increment();
    } finally {
      writeBuffers.release(buf);
    }
  }

  private void closeAssociation(MessageInfo messageInfo) {
    try {
      channel.shutdown(messageInfo.association());
    } catch (IllegalArgumentException _) {
      logger.log(TRACE, () -> "Already closed " + messageInfo.association());
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close the association " + messageInfo.association(), e);
    }
  }

  @Override
  protected InetSocketAddress sendShutdownSequence() {
    try {
      return SctpUtils.sendExitSequence(channel);
    } catch (Throwable e) {
      logger.log(ERROR, "Unable to send shutdown sequence", e);
      return null;
    }
  }
}
