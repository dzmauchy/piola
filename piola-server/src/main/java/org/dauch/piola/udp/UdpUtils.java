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

import java.io.IOException;
import java.nio.channels.DatagramChannel;

import static java.net.StandardSocketOptions.*;
import static java.net.StandardSocketOptions.SO_LINGER;

public interface UdpUtils {

  static void configure(DatagramChannel channel, ServerClientConfig config) throws IOException {
    channel.setOption(IP_MULTICAST_IF, config.multicastNetworkInterface());
    channel.setOption(IP_MULTICAST_TTL, config.multicastTtl());
    channel.setOption(IP_MULTICAST_LOOP, config.multicastLoop());
    channel.setOption(SO_REUSEPORT, true);
    channel.setOption(SO_REUSEADDR, true);
    channel.setOption(SO_RCVBUF, config.rcvBufSize());
    channel.setOption(SO_SNDBUF, config.sendBufSize());
    channel.setOption(SO_LINGER, config.linger());
    channel.configureBlocking(true);
  }
}
