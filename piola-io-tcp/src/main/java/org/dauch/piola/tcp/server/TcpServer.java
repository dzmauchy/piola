package org.dauch.piola.tcp.server;

/*-
 * #%L
 * piola-io-tcp
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
import org.dauch.piola.server.AbstractServer;
import org.dauch.piola.tcp.SocketThread;
import org.dauch.piola.tcp.TcpUtils;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.System.Logger.Level.*;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public final class TcpServer extends AbstractServer<TcpRq, TcpRs> {

  private final AtomicInteger clientCounter = new AtomicInteger();
  private final ServerSocketChannel channel;
  private final int port;
  private final TcpServerConfig config;
  private final ConcurrentSkipListMap<Integer, SocketThread> clients = new ConcurrentSkipListMap<>();

  public TcpServer(TcpServerConfig config) {
    super(config, TcpRq[]::new, TcpRs[]::new);
    this.config = config;
    try {
      channel = $("channel", ServerSocketChannel.open(config.protocolFamily()));
      channel.setOption(SO_REUSEADDR, true);
      channel.setOption(SO_RCVBUF, config.rcvBufSize());
      channel.bind(config.address(), config.backlog());
      port = ((InetSocketAddress) channel.getLocalAddress()).getPort();
      startThreads();
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to start server " + id, e));
    }
  }

  private void readHeader(ByteBuffer b, SocketChannel ch) throws IOException {
    var c = ch.read(b);
    if (c < 0) throw new ClosedChannelException();
    while (c < b.capacity()) {
      var n = ch.read(b);
      if (n < 0) throw new ClosedChannelException();
      else if (n == 0) {
        parkNanos(1_000_000L);
        if (!running) {
          throw new ClosedChannelException();
        }
      } else c += n;
    }
    receivedSize.add(8);
  }

  private void readRequest(ByteBuffer buf, SocketChannel ch, int crc) throws IOException {
    while (buf.hasRemaining()) {
      var n = ch.read(buf);
      if (n < 0) ch.close();
      else if (n == 0) {
        parkNanos(1_000_000L);
        if (!running) {
          throw new ClosedChannelException();
        }
      }
    }
    receivedSize.add(buf.flip().limit());
    receivedRequests.increment();
    var actualCrc = TcpUtils.crc(buf.slice());
    if (actualCrc != crc) {
      brokenRequests.increment();
      throw new StreamCorruptedException();
    }
  }

  private void parseRequest(ByteBuffer buf, SocketChannel ch) throws Exception {
    var context = new SerializationContext();
    var protocolId = buf.getInt();
    var _ = buf.getInt(); // reserved
    var stream = buf.getInt();
    var id = buf.getLong();
    var req = RequestFactory.request(buf, context);
    var addr = (InetSocketAddress) ch.getRemoteAddress();
    if (req.hasPayload()) {
      requests.put(new TcpRq(id, protocolId, stream, ch, addr, req, buf, context));
    } else {
      requests.put(new TcpRq(id, protocolId, stream, ch, addr, req, null, context));
      readBuffers.release(buf);
    }
    validRequests.increment();
  }

  @Override
  protected void mainLoop() {
    while (true) {
      try {
        var ch = channel.accept();
        var cli = clientCounter.getAndIncrement();
        var thread = Thread.startVirtualThread(() -> {
          try (ch) {
            TcpUtils.configure(ch, config);
            for (var b = allocateDirect(8); running; b.clear()) {
              try {
                readHeader(b, ch);
                var expectedCrc = b.getInt(0);
                var len = b.getInt(4);
                if (len < 0 || len > readBuffers.maxBufferSize()) {
                  ch.close();
                  break;
                }
                var buf = readBuffers.get().limit(len);
                try {
                  readRequest(buf, ch, expectedCrc);
                  parseRequest(buf, ch);
                } catch (Throwable e) {
                  try {
                    readBuffers.release(buf);
                  } catch (Throwable x) {
                    e.addSuppressed(x);
                  }
                  throw e;
                }
              } catch (ClosedChannelException _) {
                logger.log(INFO, () -> "Closed channel " + ch);
                break;
              } catch (StreamCorruptedException _) {
                logger.log(WARNING, () -> "Corrupted stream " + ch);
                break;
              } catch (Throwable e) {
                logger.log(ERROR, () -> "Error in channel " + ch, e);
              }
            }
          } catch (Throwable e) {
            logger.log(ERROR, () -> "Unexpected error while accepting client connection", e);
          } finally {
            clients.remove(cli);
          }
        });
        clients.put(cli, new SocketThread(ch, thread));
      } catch (ClosedChannelException _) {
        logger.log(INFO, "Closed channel");
        break;
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unexpected error while accepting client connection", e);
      }
    }
  }

  @Override
  protected void shutdown() {
    try {
      channel.close();
      for (var time = System.nanoTime(); !clients.isEmpty(); ) {
        parkNanos(1_000_000L);
        if (System.nanoTime() - time > 10_000_000_000L) {
          time = System.nanoTime();
          logger.log(INFO, () -> "Waiting for client connections to be closed: " + clients.size());
          if (logger.isLoggable(DEBUG)) {
            clients.forEach((_, st) -> {
              var exc = new RuntimeException();
              exc.setStackTrace(st.thread().getStackTrace());
              logger.log(DEBUG, () -> "Channel " + st, exc);
            });
          }
        }
      }
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close channel " + channel, e);
    }
  }

  @Override
  protected void writeResponse(TcpRq tcpRq, ByteBuffer payload, Response rs) throws Exception {
    var buf = writeBuffers.get();
    try {
      ResponseFactory.write(rs, buf
        .putInt(0) // crc
        .putInt(0) // size
        .putInt(0) // protocolId
        .putInt(id)
        .putInt(tcpRq.stream())
        .putLong(tcpRq.id())
      );
      tcpRq.context().write(buf);
      if (payload != null) {
        buf.put(payload);
      }
      buf.flip();
      buf.putInt(0, TcpUtils.crc(buf.slice(8, buf.limit() - 8)));
      buf.putInt(4, buf.limit() - 8);
      tcpRq.write(buf);
    } finally {
      writeBuffers.release(buf);
    }
  }

  @Override
  protected void reject(TcpRq tcpRq) {
    var ch = tcpRq.channel();
    try (ch) {
      logger.log(INFO, () -> "Closing " + ch);
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close client channel " + ch, e);
    }
  }

  @Override
  public Stream<InetSocketAddress> addresses() {
    try {
      var socketAddress = (InetSocketAddress) channel.getLocalAddress();
      var addr = socketAddress.getAddress();
      if (!addr.isAnyLocalAddress()) {
        return Stream.of(socketAddress);
      }
      return NetworkInterface.networkInterfaces()
        .flatMap(NetworkInterface::inetAddresses)
        .filter(a -> a.getClass() == addr.getClass())
        .map(InetSocketAddress.class::cast);
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to enumerate addresses", e);
      return Stream.empty();
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
    return port;
  }
}
