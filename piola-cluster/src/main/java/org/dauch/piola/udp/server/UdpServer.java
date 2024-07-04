package org.dauch.piola.udp.server;

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

import org.dauch.piola.api.*;
import org.dauch.piola.api.response.Response;
import org.dauch.piola.collections.buffer.BufferManager;
import org.dauch.piola.exception.DataCorruptionException;
import org.dauch.piola.server.AbstractServer;
import org.dauch.piola.udp.UdpUtils;
import org.dauch.piola.udp.fragment.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.udp.UdpUtils.*;

public final class UdpServer extends AbstractServer<UdpRq, UdpRs> {

  private static final int REQUEST_LEN_THRESHOLD = 4 + 4 + 4 + 1;

  private final long assemblyTimeout;
  private final BufferManager fragmentBuffers;
  private final InetAddress address;
  private final NetworkInterface networkInterface;
  private final DatagramChannel channel;
  private final FragmentCacheIn inFragments = new FragmentCacheIn();
  private final OutputFragments outputFragments;
  private final Thread cleanThread;
  private final ThreadPoolExecutor threadPool;

  public UdpServer(UdpServerConfig config) {
    super(config, UdpRq[]::new, UdpRs[]::new);
    assemblyTimeout = MILLISECONDS.toNanos(config.messageAssemblyTimeout());
    cleanThread = Thread.ofVirtual().name("fragmentsCleaner-" + config.id()).unstarted(this::clean);
    threadPool = threadPool(config);
    try {
      fragmentBuffers = $("fragmentBuffers", config.fragmentBuffers("server"));
      address = config.address().getAddress();
      networkInterface = config.multicastNetworkInterface();
      channel = $("channel", DatagramChannel.open(config.protocolFamily()));
      outputFragments = new OutputFragments(channel, config, fragmentBuffers);
      UdpUtils.configure(channel, config);
      channel.bind(config.address());
      UdpUtils.configureAfter(channel, config);
      startThreads();
      joinUdpGroup(config);
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to start server " + config.id(), e));
    }
  }

  private void joinUdpGroup(UdpServerConfig config) throws Exception {
    if (networkInterface == null) {
      for (var e = NetworkInterface.getNetworkInterfaces(); e.hasMoreElements(); ) {
        var itf = e.nextElement();
        if (itf.supportsMulticast()) {
          var group = config.multicastGroup();
          try {
            var membershipKey = channel.join(group, itf);
            $("membership-" + itf.getName(), membershipKey::drop);
            logger.log(INFO, () -> "Joined multicast group on " + itf.getName());
          } catch (Throwable x) {
            logger.log(WARNING, () -> "Join " + group + " error on " + itf.getName() + ": " + x.getMessage());
          }
        }
      }
    } else {
      if (networkInterface.supportsMulticast()) {
        var membershipKey = channel.join(config.multicastGroup(), networkInterface);
        $("membership", membershipKey::drop);
      } else {
        logger.log(ERROR, () -> "Multicast group not supported on " + networkInterface.getName());
      }
    }
  }

  @Override
  public Stream<InetSocketAddress> addresses() {
    try {
      if (networkInterface == null) {
        if (address.isAnyLocalAddress()) {
          return NetworkInterface.networkInterfaces()
            .flatMap(NetworkInterface::inetAddresses)
            .map(a -> new InetSocketAddress(a, getPort()));
        } else {
          return Stream.of(new InetSocketAddress(address, getPort()));
        }
      } else {
        return networkInterface.inetAddresses().map(a -> new InetSocketAddress(a, getPort()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ThreadPoolExecutor threadPool(UdpServerConfig config) {
    return new ThreadPoolExecutor(
      0, config.fragmentBufferCount(),
      1L, MINUTES,
      new SynchronousQueue<>(),
      Thread.ofVirtual().name("server-fragments-" + id + "-", 0L).factory(),
      (r, e) -> {
        try {
          e.getQueue().put(r);
        } catch (InterruptedException _) {
          logger.log(ERROR, "Interrupted while putting into the queue");
        }
      }
    );
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
      return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void mainLoop() {
    try (threadPool) {
      while (running) {
        var buf = fragmentBuffers.get();
        try {
          var addr = (InetSocketAddress) channel.receive(buf);
          if (buf.flip().limit() < REQUEST_LEN_THRESHOLD) {
            continue;
          }
          validateCrc(buf.getInt(), buf);
          var serverId = buf.getInt();
          if (serverId >= 0 && serverId != id) {
            continue;
          }
          threadPool.execute(() -> {
            try {
              var protocolId = buf.getInt();
              switch (buf.get()) {
                case RT_FRAGMENT -> processRequestFragment(protocolId, buf, addr);
                case RT_ACK -> processResponseAck(buf, addr);
                default -> brokenRequests.increment();
              }
            } catch (CancellationException e) {
              logger.log(WARNING, () -> "Fragment cancellation, address = " + addr + ": " + e.getMessage());
            } catch (DataCorruptionException e) {
              logger.log(DEBUG, "Data error", e);
            } catch (Throwable e) {
              logger.log(ERROR, "Unexpected error", e);
            } finally {
              fragmentBuffers.release(buf);
            }
          });
        } catch (ClosedChannelException _) {
          fragmentBuffers.release(buf);
          logger.log(INFO, "Closed channel");
          break;
        } catch (Throwable e) {
          fragmentBuffers.release(buf);
          logger.log(ERROR, "Unexpected error while reading to the fragment buffer", e);
        }
      }
    } catch (Throwable e) {
      logger.log(ERROR, "Unexpected error while closing the thread pool", e);
    }
    logger.log(INFO, "MainLoop finished");
  }

  private void processRequestFragment(int protocolId, ByteBuffer buf, InetSocketAddress addr) throws Exception {
    var msgKey = new MsgKey(addr, buf);
    var fragment = new Fragment(buf);
    var v = inFragments.computeByKey(msgKey, fragment);
    v.apply(fragment, buf, readBuffers);
    switch (v.tryComplete()) {
      case null -> {}
      case COMPLETED_WITH_ERROR -> v.release(readBuffers);
      case COMPLETED -> {
        try {
          var req = v.withBuffer(b -> {
            var ctx = new SerializationContext();
            var rq = RequestFactory.request(b, ctx);
            if (rq.hasPayload()) {
              return new UdpRq(msgKey, protocolId, fragment.len(), addr, rq, b, ctx);
            } else {
              readBuffers.release(b);
              return new UdpRq(msgKey, protocolId, fragment.len(), addr, rq, null, ctx);
            }
          });
          requests.put(req);
        } catch (Throwable e) {
          v.release(readBuffers);
          throw e;
        } finally {
          v.clear();
        }
      }
      default -> throw new IllegalStateException("Invalid completion state");
    }
    outputFragments.sendAck(id, protocolId, msgKey, fragment, buf, addr);
  }

  private void processResponseAck(ByteBuffer buf, InetSocketAddress addr) {
    outputFragments.handleAck(new MsgKey(addr, buf), new Fragment(buf));
  }

  @Override
  protected void reject(UdpRq udpRq) {
    inFragments.reject(udpRq.key(), readBuffers);
  }

  @Override
  protected void writeResponse(UdpRq rq, ByteBuffer payload, Response rs) throws Exception {
    var buffer = writeBuffers.get();
    try {
      ResponseFactory.write(rs, buffer);
      rq.context().write(buffer);
      if (payload != null) {
        buffer.put(payload);
      }
      outputFragments.send(rq.fragmentLength(), id, rq.protocolId(), rq.stream(), rq.id(), buffer.flip(), rq.address());
    } finally {
      writeBuffers.release(buffer);
    }
  }

  private void clean() {
    var thread = Thread.currentThread();
    while (!thread.isInterrupted()) {
      inFragments.clean(readBuffers, assemblyTimeout);
      parkNanos(assemblyTimeout >> 1);
    }
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
  protected void startThreads() {
    cleanThread.start();
    $("fragments-cleaner", () -> {
      cleanThread.interrupt();
      cleanThread.join();
    });
    super.startThreads();
  }
}
