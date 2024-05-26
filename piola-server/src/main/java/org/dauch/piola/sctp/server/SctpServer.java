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
import org.dauch.piola.api.RequestFactory;
import org.dauch.piola.api.SerializationContext;
import org.dauch.piola.api.response.ErrorResponse;
import org.dauch.piola.api.response.Response;
import org.dauch.piola.exception.ExceptionData;
import org.dauch.piola.sctp.SctpUtils;
import org.dauch.piola.server.AbstractServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.sun.nio.sctp.MessageInfo.createOutgoing;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

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
      throw initException(new IllegalStateException("Unable to start server " + id));
    }
  }

  @Override
  protected void doSendShutdownSequence() throws Exception {
    var addr = SctpUtils.sendExitSequence(channel, exitCmd);
    logger.log(INFO, () -> "Shutdown sequence sent to " + addr);
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
  protected void doInMainLoop(ByteBuffer buf) throws Exception {
    var msg = channel.receive(buf, null, null);
    receivedSize.add(msg.bytes());
    receivedRequests.increment();
    if (SctpUtils.isExitSequence(channel, msg, buf, exitCmd)) {
      logger.log(INFO, () -> "Termination sequence received " + msg);
      throw new InterruptedException();
    } else if (msg.isComplete()) {
      try {
        read(msg, buf);
        validRequests.increment();
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Exception " + msg, e);
        brokenRequests.increment();
        closeAssociation(msg);
      }
    } else {
      logger.log(ERROR, () -> "Incomplete message: " + msg);
      closeAssociation(msg);
      incompleteRequests.increment();
    }
  }

  private void read(MessageInfo msg, ByteBuffer buffer) throws Throwable {
    var context = new SerializationContext();
    var id = buffer.flip().getLong();
    var req = RequestFactory.request(buffer, context);
    if (req.hasPayload()) {
      requests.put(new SctpRq(id, msg, req, buffer, context));
    } else {
      buffers.release(buffer);
      requests.put(new SctpRq(id, msg, req, null, context));
    }
  }

  @Override
  protected void requestLoop() {
    while (runningRequests) {
      drainRequests(this::process);
    }
  }

  private void process(SctpRq r) {
    var rsc = (Consumer<? super Response>) rs -> {
      try {
        responses.put(new SctpRs(r.id(), r.meta(), rs, r.context()));
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unexpected exception", e);
        closeAssociation(r.meta());
      }
    };
    try {
      doProcess(r, rsc);
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to process request " + r, e);
      rsc.accept(new ErrorResponse("Unknown error", ExceptionData.from(e)));
    }
  }

  @Override
  protected void responseLoop() {
    while (runningResponses) {
      drainResponses(this::writeResponse);
    }
  }

  private void writeResponse(SctpRs el) {
    var inp = el.message();
    try {
      var msg = createOutgoing(inp.association(), inp.address(), inp.streamNumber());
      var buf = writeBuffers.get().putInt(id);
      try {
        var count = channel.send(el.write(buf), msg);
        sentSize.add(count);
        sentMessages.increment();
      } finally {
        writeBuffers.release(buf);
      }
    } catch (Throwable e) {
      closeAssociation(inp);
      logger.log(ERROR, () -> "Unable to write response to " + inp, e);
    }
  }

  private void closeAssociation(MessageInfo messageInfo) {
    try {
      channel.shutdown(messageInfo.association());
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close the association " + messageInfo.association(), e);
    }
  }
}
