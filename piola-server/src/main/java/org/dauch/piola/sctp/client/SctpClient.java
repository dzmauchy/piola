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

import com.sun.nio.sctp.SctpMultiChannel;
import org.dauch.piola.api.*;
import org.dauch.piola.api.request.Request;
import org.dauch.piola.api.response.ErrorResponse;
import org.dauch.piola.api.response.UnknownResponse;
import org.dauch.piola.client.AbstractClient;
import org.dauch.piola.client.ClientResponse;
import org.dauch.piola.sctp.SctpUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static com.sun.nio.sctp.MessageInfo.createOutgoing;
import static java.lang.System.Logger.Level.INFO;
import static org.dauch.piola.sctp.SctpUtils.streamNumber;

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
      throw initException(new IllegalStateException("Unable to create client " + conf.name(), e));
    }
  }

  @Override
  protected void sendShutdownSequence() throws Exception {
    var addr = SctpUtils.sendExitSequence(channel, exitCmd);
    logger.log(INFO, () -> "Shutdown sequence sent to " + addr);
  }

  @Override
  protected void doScanResponses(ByteBuffer buf) throws Exception {
    var msg = channel.receive(buf, null, null);
    try {
      if (SctpUtils.isExitSequence(channel, msg, buf, exitCmd)) {
        logger.log(INFO, () -> "Shutdown sequence received: " + msg);
        throw new InterruptedException();
      }
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
        channel.shutdown(msg.association());
      } else {
        var in = new SerializationContext();
        var rsp = ResponseFactory.read(buf, in);
        switch (rsp) {
          case ErrorResponse _ -> errorResponses.increment();
          case UnknownResponse _ -> unknownResponses.increment();
          default -> {}
        }
        var out = SerializationContext.read(buf);
        var address = (InetSocketAddress) msg.address();
        queue.add(new ClientResponse<>(serverId, address, rsp, out, in));
      }
    } catch (Throwable e) {
      unexpectedErrors.increment();
      channel.shutdown(msg.association());
      throw e;
    }
  }

  @Override
  protected void send(ByteBuffer buf, long id, Request<?> rq, InetSocketAddress addr) throws Exception {
    RequestFactory.write(rq, buf.putLong(id));
    var size = channel.send(buf.flip(), createOutgoing(addr, streamNumber(rq)));
    sentRequests.increment();
    sentSize.add(size);
  }
}
