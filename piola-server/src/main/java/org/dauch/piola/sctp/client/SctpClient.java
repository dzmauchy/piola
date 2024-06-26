package org.dauch.piola.sctp.client;

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
import org.dauch.piola.api.request.Request;
import org.dauch.piola.client.AbstractClient;
import org.dauch.piola.sctp.SctpUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static com.sun.nio.sctp.MessageInfo.createOutgoing;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class SctpClient extends AbstractClient {

  private final SctpMultiChannel channel;

  public SctpClient(SctpClientConfig conf) {
    super(conf);
    try {
      channel = $("channel", SctpMultiChannel.open());
      SctpUtils.configure(channel, conf);
      channel.bind(null, 0);
      startThreads();
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to create client " + conf.name(), e));
    }
  }

  @Override
  protected void scanResponses() {
    while (true) {
      var buf = buffers.get();
      try {
        var msg = channel.receive(buf, null, null);
        if (!running) {
          throw new InterruptedException("Not running");
        }
        readResponse(buf, msg);
      } catch (ClosedChannelException | InterruptedException e) {
        break;
      } catch (Throwable e) {
        logger.log(ERROR, "Unexpected error", e);
      } finally {
        buffers.release(buf);
      }
    }
    logger.log(INFO, "Main loop closed");
  }

  private void readResponse(ByteBuffer buf, MessageInfo msg) throws Exception {
    try {
      receivedResponses.increment();
      receivedSize.add(msg.bytes());
      if (!msg.isComplete()) {
        incompleteResponses.increment();
        channel.shutdown(msg.association());
        return;
      }
      var serverId = buf.flip().getInt();
      var id = buf.getLong();
      var queue = responses.get(id);
      if (queue == null) {
        forgottenResponses.increment();
      } else {
        var addr = (InetSocketAddress) msg.address();
        var response = clientResponse(buf, msg.payloadProtocolID(), serverId, msg.streamNumber(), addr);
        queue.add(response);
      }
    } catch (Throwable e) {
      unexpectedErrors.increment();
      channel.shutdown(msg.association());
      throw e;
    }
  }

  @Override
  protected void fill(ByteBuffer buf, long id, Request<?> rq, ByteBuffer payload) {
    RequestFactory.write(rq, buf.putLong(id));
    if (payload != null) {
      buf.put(payload);
    }
  }

  @Override
  protected int send(ByteBuffer buf, Request<?> rq, int stream, long id, InetSocketAddress address) throws Exception {
    var size = channel.send(buf, createOutgoing(address, stream));
    sentRequests.increment();
    sentSize.add(size);
    return 1;
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
