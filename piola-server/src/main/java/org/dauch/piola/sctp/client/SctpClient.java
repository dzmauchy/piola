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
import org.dauch.piola.api.response.Response;
import org.dauch.piola.client.*;
import org.dauch.piola.sctp.SctpUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedTransferQueue;

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
      var serverId = buf.flip().getInt();
      var id = buf.getLong();
      var queue = responses.get(id);
      if (queue == null) {
        channel.shutdown(msg.association());
      } else {
        var in = new SerializationContext();
        var rsp = ResponseFactory.read(buf, in);
        var out = SerializationContext.read(buf);
        var address = (InetSocketAddress) msg.address();
        queue.add(new ClientResponse<>(serverId, address, rsp, out, in));
      }
    } catch (Throwable e) {
      channel.shutdown(msg.association());
      throw e;
    }
  }

  @Override
  public <RQ extends Request<RS>, RS extends Response> SimpleResponses<RS> send(RQ rq, InetSocketAddress address) {
    var id = ids.getAndIncrement();
    var fetcher = new SimpleResponses<RS>(id, responses.computeIfAbsent(id, _ -> new LinkedTransferQueue<>()));
    var responses = this.responses;
    CLEANER.register(fetcher, () -> responses.remove(id));
    var buf = writeBuffers.get();
    try {
      RequestFactory.write(rq, buf.putLong(id));
      channel.send(buf.flip(), createOutgoing(address, streamNumber(rq)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      writeBuffers.release(buf);
    }
    return fetcher;
  }
}
