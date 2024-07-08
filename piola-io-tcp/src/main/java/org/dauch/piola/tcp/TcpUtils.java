package org.dauch.piola.tcp;

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

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.zip.CRC32;

import static java.net.InetSocketAddress.createUnresolved;
import static java.net.StandardProtocolFamily.INET;
import static java.net.StandardProtocolFamily.INET6;
import static java.net.StandardSocketOptions.*;
import static java.nio.channels.SocketChannel.open;

public interface TcpUtils {

  static void configure(NetworkChannel channel, CommonConfig config) throws IOException {
    channel.setOption(SO_RCVBUF, config.rcvBufSize());
    if (channel instanceof ServerSocketChannel) {
      channel.setOption(SO_REUSEADDR, true);
    } else {
      channel.setOption(SO_SNDBUF, config.sendBufSize());
      channel.setOption(TCP_NODELAY, config.nagle());
      channel.setOption(SO_LINGER, config.linger());
      channel.setOption(SO_KEEPALIVE, config.keepAlive());
      if (channel instanceof SocketChannel sc) {
        sc.configureBlocking(false);
      }
    }
  }

  static InetSocketAddress sendExitSequence(SocketChannel channel) throws IOException {
    var socketAddress = (InetSocketAddress) channel.getLocalAddress();
    var address = socketAddress.getAddress();
    var port = socketAddress.getPort();
    var destination = address.isAnyLocalAddress()
      ? (address instanceof Inet4Address ? createUnresolved("127.0.0.1", port) : createUnresolved("::1", port))
      : socketAddress;
    try (var client = open(address instanceof Inet6Address ? INET6 : INET)) {
      if (client.connect(destination)) {
        var written = client.write(ByteBuffer.allocate(Integer.BYTES).putInt(0, -1));
        if (written != Integer.BYTES) {
          throw new StreamCorruptedException("Failed to write exit sequence");
        }
      } else {
        throw new IllegalStateException("Failed to connect to " + destination);
      }
    }
    return destination;
  }

  static int crc(ByteBuffer buffer) {
    var crc = new CRC32();
    crc.update(buffer.slice());
    return (int) crc.getValue();
  }
}
