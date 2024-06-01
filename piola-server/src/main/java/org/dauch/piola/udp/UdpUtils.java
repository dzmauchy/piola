package org.dauch.piola.udp;

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

import org.dauch.piola.exception.DataCorruptionException;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.zip.CRC32;

import static java.net.StandardProtocolFamily.INET;
import static java.net.StandardProtocolFamily.INET6;
import static java.net.StandardSocketOptions.*;

public interface UdpUtils {

  static void configure(DatagramChannel channel, ServerClientConfig config) throws IOException {
    channel.setOption(SO_REUSEPORT, true);
    channel.setOption(SO_REUSEADDR, true);
    channel.setOption(SO_RCVBUF, config.rcvBufSize());
    channel.setOption(SO_SNDBUF, config.sendBufSize());
    channel.setOption(SO_LINGER, config.linger());
    channel.configureBlocking(true);
  }

  static void configureAfter(DatagramChannel channel, ServerClientConfig config) throws IOException {
    channel.setOption(IP_MULTICAST_TTL, config.multicastTtl());
    channel.setOption(IP_MULTICAST_IF, config.multicastNetworkInterface());
    channel.setOption(IP_MULTICAST_LOOP, config.multicastLoop());
  }

  static boolean isExitSequence(DatagramChannel ch, InetSocketAddress a, ByteBuffer buf, long cmd) throws IOException {
    var thisAddr = (InetSocketAddress) ch.getLocalAddress();
    if (thisAddr.getPort() != a.getPort()) {
      return false;
    }
    if (!a.getAddress().isLoopbackAddress()) {
      return false;
    }
    if (buf.remaining() != Long.BYTES) {
      return false;
    }
    var actual = buf.getLong(buf.position());
    return actual == cmd;
  }

  static void sendExitSequence(DatagramChannel channel, long cmd) throws IOException {
    var addr = (InetSocketAddress) channel.getLocalAddress();
    var port = addr.getPort();
    var family = (addr.getAddress() instanceof Inet4Address) ? INET : INET6;
    try (var newChannel = DatagramChannel.open(family)) {
      newChannel.setOption(SO_REUSEPORT, true);
      newChannel.setOption(SO_REUSEADDR, true);
      newChannel.configureBlocking(true);
      if (family == INET) {
        newChannel.bind(new InetSocketAddress(Inet4Address.ofLiteral("127.0.0.1"), port));
      } else {
        newChannel.bind(new InetSocketAddress(Inet6Address.ofLiteral("::1"), port));
      }
      if (addr.getAddress().isAnyLocalAddress()) {
        newChannel.connect(newChannel.getLocalAddress());
      } else {
        newChannel.connect(channel.getLocalAddress());
      }
      newChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(0, cmd));
    }
  }

  static void validateCrc(ByteBuffer buffer) {
    var checksum = buffer.getInt();
    var crc = new CRC32();
    buffer.mark();
    crc.update(buffer);
    buffer.reset();
    if (Integer.toUnsignedLong(checksum) != crc.getValue())
      throw new DataCorruptionException("checksum mismatch", null);
  }

  static int crc(ByteBuffer buffer) {
    var crc = new CRC32();
    crc.update(buffer);
    return (int) crc.getValue();
  }
}
