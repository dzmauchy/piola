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

import org.dauch.piola.api.RequestFactory;
import org.dauch.piola.api.SerializationContext;
import org.dauch.piola.buffer.BufferManager;
import org.dauch.piola.exception.DataCorruptionException;
import org.dauch.piola.server.AbstractServer;
import org.dauch.piola.udp.UdpUtils;
import org.dauch.piola.udp.fragment.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.*;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.udp.UdpUtils.validateCrc;

public final class UdpServer extends AbstractServer<UdpRq, UdpRs> {

  private final int fragmentTimeout;
  private final BufferManager fragmentBuffers;
  private final InetAddress address;
  private final NetworkInterface networkInterface;
  private final DatagramChannel channel;
  private final FragmentCache inFragments = new FragmentCache();
  private final FragmentCache outFragments = new FragmentCache();
  private final Thread cleanThread;

  public UdpServer(UdpServerConfig config) {
    super(config, UdpRq[]::new, UdpRs[]::new);
    fragmentTimeout = config.fragmentTimeout();
    cleanThread = Thread.ofVirtual().name("fragmentsCleaner-" + config.id()).unstarted(this::clean);
    try {
      fragmentBuffers = $("fragmentBuffers", config.fragmentBuffers());
      address = config.address().getAddress();
      networkInterface = config.multicastNetworkInterface();
      channel = $("channel", DatagramChannel.open(config.protocolFamily()));
      UdpUtils.configure(channel, config);
      channel.bind(config.address());
      UdpUtils.configureAfter(channel, config);
      var membershipKey = channel.join(config.multicastGroup(), networkInterface);
      startThreads();
      $("membership", membershipKey::drop);
    } catch (Throwable e) {
      throw initException(new IllegalStateException("Unable to start server " + config.id(), e));
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
    while (true) {
      var buf = fragmentBuffers.get();
      try {
        var addr = (InetSocketAddress) channel.receive(buf);
        if (UdpUtils.isExitSequence(channel, addr, buf.flip(), exitCmd)) {
          logger.log(INFO, () -> "Exit sequence received from " + addr);
          break;
        }
        Thread.startVirtualThread(() -> {
          try {
            receivedSize.add(buf.remaining());
            validateCrc(buf);
            switch (buf.get()) {
              case 1 -> processRequestFragment(buf, addr);
              case 2 -> processRequestAck(buf, addr);
              case 3 -> processResponseAck(buf, addr);
              default -> brokenRequests.increment();
            }
          } catch (DataCorruptionException e) {
            logger.log(DEBUG, "Data error", e);
            brokenRequests.increment();
          } catch (BufferUnderflowException _) {
            brokenRequests.increment();
          } catch (Throwable e) {
            logger.log(ERROR, "Unexpected error", e);
          } finally {
            fragmentBuffers.release(buf);
          }
        });
      } catch (ClosedChannelException _) {
        logger.log(INFO, "Closed");
        break;
      } catch (Throwable e) {
        logger.log(ERROR, "Unexpected error", e);
      }
    }
  }

  private void processRequestFragment(ByteBuffer buf, InetSocketAddress addr) throws IOException {
    var fragment = new Fragment(buf);
    var cache = inFragments.fragmentsBy(addr);
    var v = cache.computeIfAbsent(fragment.key(), _ -> new MsgValue(fragment));
    v.validate(fragment);
    v.apply(fragment, buf, buffers::get);
    synchronized (v) {
      v.prepareAck(fragment, buf);
      channel.send(buf, addr);
    }
  }

  private void processRequestAck(ByteBuffer buf, InetSocketAddress addr) throws Exception {
    var fragment = new Fragment(buf);
    var cache = inFragments.fragmentsBy(addr);
    var msg = cache.get(fragment.key());
    if (msg == null) {
      return;
    }
    msg.validate(fragment);
    msg.applyRemote(fragment);
    if (msg.tryComplete()) {
      var ctx = new SerializationContext();
      var req = RequestFactory.request(msg.slice(), ctx);
      if (req.hasPayload()) {
        requests.put(msg.withRawBuffer(b -> new UdpRq(fragment.key(), addr, req, b, ctx)));
      } else {
        msg.release(buffers);
        requests.put(new UdpRq(fragment.key(), addr, req, null, ctx));
      }
      synchronized (msg) {
        msg.prepareAck(fragment, buf);
        channel.send(buf, addr);
      }
    } else if (msg.isCompleted()) {
      synchronized (msg) {
        msg.prepareAck(fragment, buf);
        channel.send(buf, addr);
      }
    }
  }

  private void processResponseAck(ByteBuffer buf, InetSocketAddress addr) {

  }

  @Override
  protected void doSendShutdownSequence() throws Exception {
    UdpUtils.sendExitSequence(channel, exitCmd);
  }

  @Override
  protected void requestLoop() {
    while (runningRequests) {
      drainRequests(this::process);
    }
  }

  private void process(UdpRq rq) {

  }

  @Override
  protected void responseLoop() {
    while (runningResponses) {
      drainResponses(this::writeResponse);
    }
  }

  private void writeResponse(UdpRs rs) {

  }

  private void clean() {
    var thread = Thread.currentThread();
    var timeout = TimeUnit.SECONDS.toNanos(fragmentTimeout);
    while (!thread.isInterrupted()) {
      incompleteRequests.add(inFragments.clean(buffers, timeout));
      incompleteResponses.add(outFragments.clean(buffers, timeout));
      parkNanos(timeout >> 1);
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
