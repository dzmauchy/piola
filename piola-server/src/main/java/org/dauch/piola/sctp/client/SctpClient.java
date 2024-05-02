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
import org.dauch.piola.buffer.BufferManager;
import org.dauch.piola.client.*;
import org.dauch.piola.sctp.SctpUtils;
import org.dauch.piola.util.Closeables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.sun.nio.sctp.MessageInfo.createOutgoing;
import static java.lang.Character.MAX_RADIX;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dauch.piola.sctp.SctpUtils.streamNumber;

public final class SctpClient implements Client {

  private static final Cleaner CLEANER = Cleaner.create(Thread.ofVirtual().name("client-cleaner").factory());

  private final long exitCmd = ThreadLocalRandom.current().nextLong();
  private final AtomicLong ids = new AtomicLong();
  private final ConcurrentSkipListMap<Long, LinkedTransferQueue<ClientResponse<?>>> responses = new ConcurrentSkipListMap<>();
  private final SctpClientConfig config;
  private final System.Logger logger;
  private final SctpMultiChannel channel;
  private final BufferManager buffers;
  private final BufferManager writeBuffers;
  private final Thread responseScanner;

  public SctpClient(SctpClientConfig conf) {
    config = conf;
    logger = System.getLogger("client-" + conf.name());
    var cs = new Closeables();
    try {
      var prefix = new BigInteger(1, conf.name().getBytes(UTF_8)).toString(MAX_RADIX);
      buffers = cs.add(new BufferManager(prefix + "-read", conf));
      writeBuffers = cs.add(new BufferManager(prefix + "-write", conf));
      channel = SctpMultiChannel.open();
      SctpUtils.configure(channel, conf);
      channel.bind(null, 0);
      responseScanner = Thread.ofVirtual().name("client-" + conf.name()).start(this::scanForResponses);
    } catch (Throwable e) {
      throw cs.closeAndWrap(new IllegalStateException("Unable to create client " + conf.name(), e));
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

  private void scanForResponses() {
    while (true) {
      var buf = buffers.get();
      try {
        var msg = channel.receive(buf, null, null);
        try {
          if (SctpUtils.isExitSequence(channel, msg, buf, exitCmd)) {
            logger.log(INFO, () -> "Shutdown sequence received: " + msg);
            break;
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
      } catch (ClosedChannelException _) {
        logger.log(INFO, "Closed");
        break;
      } catch (Throwable e) {
        logger.log(ERROR, "Unknown error", e);
      } finally {
        buffers.release(buf);
      }
    }
    logger.log(INFO, "Scanner finished");
  }

  @Override
  public void close() throws Exception {
    try (buffers; writeBuffers; channel) {
      logger.log(INFO, () -> "Closing " + config.name());
      var addr = SctpUtils.sendExitSequence(channel, exitCmd);
      logger.log(INFO, () -> "Shutdown sequence sent to " + addr);
      responseScanner.join();
    }
    logger.log(INFO, () -> "Closed " + config.name());
  }
}
