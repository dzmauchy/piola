package org.dauch.piola.udp.client;

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

import org.dauch.piola.api.RequestFactory;
import org.dauch.piola.api.request.Request;
import org.dauch.piola.buffer.BufferManager;
import org.dauch.piola.client.AbstractClient;
import org.dauch.piola.exception.DataCorruptionException;
import org.dauch.piola.udp.UdpUtils;
import org.dauch.piola.udp.fragment.*;

import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.*;

import static java.lang.System.Logger.Level.*;
import static org.dauch.piola.udp.UdpUtils.*;

public final class UdpClient extends AbstractClient {

  private final int fragmentPayloadSize;
  private final BufferManager fragmentBuffers;
  private final DatagramChannel channel;
  private final FragmentCacheIn inFragments = new FragmentCacheIn();
  private final OutputFragments outputFragments;
  private final ThreadPoolExecutor threadPool;

  public UdpClient(UdpClientConfig config) {
    super(config);
    fragmentPayloadSize = config.fragmentPayloadSize();
    threadPool = new ThreadPoolExecutor(
      0, config.fragmentBufferCount(),
      1L, TimeUnit.MINUTES,
      new SynchronousQueue<>(),
      Thread.ofVirtual().name("client-fragments-" + config.name() + "-", 0L).factory(),
      new ThreadPoolExecutor.CallerRunsPolicy()
    );
    try {
      fragmentBuffers = $("fragment-buffers", config.fragmentBuffers("client"));
      channel = $("channel", DatagramChannel.open(config.protocolFamily()));
      $("inFragments", () -> {
        var count = inFragments.close(buffers);
        logger.log(INFO, () -> "Cleaned up buffers: " + count);
      });
      outputFragments = new OutputFragments(channel, config, fragmentBuffers);
      UdpUtils.configure(channel, config);
      channel.bind(config.address());
      UdpUtils.configureAfter(channel, config);
      startThreads();
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to start " + config.name(), e));
    }
  }

  @Override
  protected void scanResponses() {
    while (true) {
      var buf = fragmentBuffers.get();
      try {
        var addr = (InetSocketAddress) channel.receive(buf);
        if (!running) {
          throw new InterruptedException("Not running");
        }
        threadPool.execute(() -> {
          try {
            validateCrc(buf.flip().getInt(), buf);
            var protocolId = buf.getInt();
            var serverId = buf.getInt();
            switch (buf.get()) {
              case RT_FRAGMENT -> processFragment(buf, protocolId, serverId, addr);
              case RT_ACK -> processAck(buf, addr);
              default -> throw new DataCorruptionException("Unknown command", null);
            }
          } catch (ClosedChannelException _) {
            logger.log(INFO, "Closed channel");
          } catch (DataCorruptionException e) {
            logger.log(DEBUG, "Data error", e);
          } catch (BufferUnderflowException e) {
            logger.log(DEBUG, "Buffer underflow", e);
          } catch (Throwable e) {
            logger.log(ERROR, "Unexpected error", e);
          } finally {
            fragmentBuffers.release(buf);
          }
        });
      } catch (ClosedChannelException | InterruptedException _) {
        fragmentBuffers.release(buf);
        logger.log(INFO, "Closed");
        break;
      } catch (Throwable e) {
        fragmentBuffers.release(buf);
        logger.log(ERROR, "Unexpected error", e);
      }
    }
    logger.log(INFO, "Main loop finished");
  }

  private void processFragment(ByteBuffer buf, int protocolId, int serverId, InetSocketAddress address) throws Exception {
    var msgKey = new MsgKey(address, buf);
    var queue = responses.get(msgKey.id());
    if (queue == null) {
      var v = inFragments.getByKey(msgKey);
      if (v != null && v.release(buffers)) {
        forgottenResponses.increment();
      }
      return;
    }
    var fragment = new Fragment(buf);
    var v = inFragments.computeByKey(msgKey, fragment);
    v.apply(fragment, buf, buffers);
    switch (v.tryComplete()) {
      case null -> {}
      case COMPLETED_WITH_ERROR -> v.release(buffers);
      case COMPLETED -> {
        try {
          var rsp = v.withBuffer(b -> {
            try {
              return clientResponse(b, protocolId, serverId, msgKey.stream(), address);
            } finally {
              buffers.release(b);
            }
          });
          receivedResponses.increment();
          receivedSize.add(v.size);
          queue.add(rsp);
        } finally {
          v.clear();
        }
      }
      default -> throw new IllegalStateException("Invalid completion state");
    }
    outputFragments.sendAck(serverId, protocolId, msgKey, fragment, buf, address);
  }

  private void processAck(ByteBuffer buf, InetSocketAddress address) {
    outputFragments.handleAck(new MsgKey(address, buf), new Fragment(buf));
  }

  @Override
  protected void fill(ByteBuffer buf, long id, Request<?> rq, ByteBuffer payload) {
    RequestFactory.write(rq, buf);
    if (payload != null) {
      buf.put(payload);
    }
  }

  @Override
  protected int send(ByteBuffer buffer, Request<?> rq, int stream, long id, InetSocketAddress address) throws Exception {
    return outputFragments.send(fragmentPayloadSize, -1, 0, stream, id, buffer, address);
  }

  @Override
  protected InetSocketAddress sendShutdownSequence() {
    try {
      return UdpUtils.sendExitSequence(channel);
    } catch (Throwable e) {
      logger.log(ERROR, "Unable to send shutdown sequence", e);
      return null;
    }
  }

  @Override
  protected void closeMainLoop() {
    try (threadPool) {
      super.closeMainLoop();
    }
  }
}
