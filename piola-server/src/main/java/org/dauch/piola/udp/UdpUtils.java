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

  byte RT_FRAGMENT = 1;
  byte RT_ACK = 2;

  static void configure(DatagramChannel channel, ServerClientConfig config) throws IOException {
    var supportedOptions = channel.supportedOptions();
    if (supportedOptions.contains(SO_REUSEPORT)) {
      channel.setOption(SO_REUSEPORT, true);
    }
    channel.setOption(SO_REUSEADDR, true);
    channel.setOption(SO_RCVBUF, config.rcvBufSize());
    channel.setOption(SO_SNDBUF, config.sendBufSize());
    channel.configureBlocking(true);
  }

  static void configureAfter(DatagramChannel channel, ServerClientConfig config) throws IOException {
    channel.setOption(IP_MULTICAST_TTL, config.multicastTtl());
    channel.setOption(IP_MULTICAST_LOOP, config.multicastLoop());
  }

  static InetSocketAddress localAddress(StandardProtocolFamily family, int port) {
    return switch (family) {
      case INET -> new InetSocketAddress("127.0.0.1", port);
      case INET6 -> new InetSocketAddress("::1", port);
      default -> throw new IllegalArgumentException(family.name());
    };
  }

  static InetSocketAddress sendExitSequence(DatagramChannel channel) throws IOException {
    var addr = (InetSocketAddress) channel.getLocalAddress();
    var port = addr.getPort();
    var family = (addr.getAddress() instanceof Inet4Address) ? INET : INET6;
    var buf = ByteBuffer.allocate(1);
    if (addr.getAddress().isAnyLocalAddress()) {
      addr = localAddress(family, port);
    }
    channel.send(buf, addr);
    return addr;
  }

  static void validateCrc(int checksum, ByteBuffer buffer) {
    var crc = new CRC32();
    crc.update(buffer.slice());
    if (Integer.toUnsignedLong(checksum) != crc.getValue())
      throw new DataCorruptionException("checksum mismatch", null);
  }

  static int crc(ByteBuffer buffer) {
    var crc = new CRC32();
    crc.update(buffer);
    return (int) crc.getValue();
  }
}
