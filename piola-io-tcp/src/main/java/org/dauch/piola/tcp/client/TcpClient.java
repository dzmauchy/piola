package org.dauch.piola.tcp.client;

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

import org.dauch.piola.api.RequestFactory;
import org.dauch.piola.api.request.Request;
import org.dauch.piola.client.AbstractClient;
import org.dauch.piola.tcp.TcpUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.nanoTime;
import static java.nio.ByteBuffer.allocateDirect;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.tcp.TcpUtils.crc;

public final class TcpClient extends AbstractClient {

  private final ConcurrentHashMap<InetSocketAddress, SocketChannel> channels = new ConcurrentHashMap<>(128, 0.9f);
  private final TcpClientConfig config;

  public TcpClient(TcpClientConfig config) {
    super(config);
    this.config = config;
    startThreads();
  }

  @Override
  protected void scanResponses() {
    // TODO: evict old channels
  }

  @Override
  protected void fill(ByteBuffer buf, long id, int stream, Request<?> rq, ByteBuffer payload) {
    RequestFactory.write(rq, buf
      .putInt(0) // crc
      .putInt(0) // size
      .putInt(0) // protocolId
      .putInt(0) // reserved
      .putInt(stream)
      .putLong(id)
    );
    if (payload != null) {
      buf.put(payload);
    }
    buf.putInt(0, crc(buf.slice(8, buf.position() - 8)));
    buf.putInt(4, buf.position() - 8);
  }

  private void write(ByteBuffer buf, SocketChannel ch) throws IOException {
    while (buf.hasRemaining()) {
      var n = ch.write(buf);
      if (n < 0) {
        throw new ClosedChannelException();
      } else if (n == 0) {
        parkNanos(100_000L);
      }
    }
  }

  @Override
  protected int send(ByteBuffer buf, Request<?> rq, int stream, long id, InetSocketAddress address) throws Exception {
    var ch = channel(address);
    var size = buf.limit();
    synchronized (ch.blockingLock()) {
      write(buf, ch);
    }
    sentRequests.increment();
    sentSize.add(size);
    return size;
  }

  private void read(ByteBuffer b, SocketChannel ch) throws IOException {
    while (b.hasRemaining()) {
      var n = ch.read(b);
      if (n < 0) {
        throw new ClosedChannelException();
      } else if (n == 0) {
        parkNanos(1_000_000L);
      }
    }
  }

  private void listenChannel(SocketChannel c, InetSocketAddress a) {
    try (c) {
      for (var b = allocateDirect(8); c.isConnected() && c.isOpen(); b.clear()) {
        try {
          read(b, c);
          var crc = b.getInt(0);
          var len = b.getInt(4);
          var buf = buffers.get().limit(len);
          try {
            read(buf, c);
            var actualCrc = crc(buf.flip().slice());
            var protocolId = buf.getInt();
            var serverId = buf.getInt();
            var stream = buf.getInt();
            var id = buf.getLong();
            if (actualCrc != crc) {
              throw new StreamCorruptedException("CRC error");
            }
            var queue = responses.get(id);
            if (queue == null) {
              forgottenResponses.increment();
            } else {
              queue.add(clientResponse(buf, protocolId, serverId, stream, a));
            }
          } finally {
            buffers.release(buf);
          }
        } catch (ClosedChannelException _) {
          break;
        } catch (StreamCorruptedException e) {
          if (channels.remove(a, c)) {
            try {
              c.close();
            } catch (Throwable x) {
              e.addSuppressed(x);
            }
          }
          brokenResponses.increment();
          logger.log(ERROR, () -> "CRC error: " + c, e);
        } catch (Throwable e) {
          unexpectedErrors.increment();
          logger.log(ERROR, () -> "Unexpected exception", e);
        }
      }
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unexpected exception on " + a, e);
    } finally {
      channels.remove(a, c);
    }
  }

  private SocketChannel channel(InetSocketAddress address) {
    return channels.computeIfAbsent(address, a -> {
      try {
        var c = SocketChannel.open(config.protocolFamily());
        TcpUtils.configure(c, config);
        if (!c.connect(a)) {
          for (long t = nanoTime(), mt = t + 10_000_000_000L; !c.finishConnect(); parkNanos(100_000L)) {
            if (nanoTime() > mt) throw new SocketTimeoutException("Connection timeout");
          }
        }
        Thread.ofVirtual()
          .name(config.name() + ":" + address)
          .start(() -> listenChannel(c, a));
        return c;
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
  }

  @Override
  protected void shutdown() {
    channels.forEach((addr, ch) -> {
      try {
        ch.close();
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to close channel " + addr, e);
      }
    });
    channels.clear();
  }
}
