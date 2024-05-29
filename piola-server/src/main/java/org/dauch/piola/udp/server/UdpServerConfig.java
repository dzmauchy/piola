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

import org.dauch.piola.annotation.Conf;
import org.dauch.piola.annotation.Default;
import org.dauch.piola.api.conf.UdpServerConfigIO;
import org.dauch.piola.server.ServerConfig;
import org.dauch.piola.udp.ServerClientConfig;

import java.net.*;
import java.nio.file.Path;
import java.util.Properties;

@Conf
public record UdpServerConfig(
  @Default("0") int id,
  @Default("INET") StandardProtocolFamily protocolFamily,
  @Default("addressDefault(protocolFamily)") InetSocketAddress address,
  @Default("64") int backlog,
  @Default("60") int linger,
  @Default("1 << 20") int rcvBufSize,
  @Default("1 << 20") int sendBufSize,
  @Default("2 << 20") int maxMessageSize,
  @Default("256") int queueSize,
  @Default("64") int bufferCount,
  @Default("0.25f") float freeRatio,
  @Default("bufferDirDefault()") Path bufferDir,
  @Default("baseDirDefault()") Path baseDir,
  NetworkInterface multicastNetworkInterface,
  @Default("255") int multicastTtl,
  @Default("true") boolean multicastLoop,
  @Default("multicastGroupDefault()") InetAddress multicastGroup
) implements ServerClientConfig, ServerConfig {

  public static UdpServerConfig fromProperties(String prefix, Properties properties) {
    return UdpServerConfigIO.get(prefix, properties);
  }

  public static UdpServerConfig fromProperties(String prefix) {
    return fromProperties(prefix, System.getProperties());
  }

  public static UdpServerConfig fromProperties() {
    return fromProperties("piola.server");
  }

  public static InetSocketAddress addressDefault(StandardProtocolFamily protocolFamily) {
    return switch (protocolFamily) {
      case INET6 -> new InetSocketAddress(Inet4Address.ofLiteral("::"), 0);
      case INET -> new InetSocketAddress(Inet6Address.ofLiteral("0.0.0.0"), 0);
      case UNIX -> throw new UnsupportedOperationException("Unsupported protocol family: " + protocolFamily);
    };
  }

  public static Path bufferDirDefault() {
    return Path.of(System.getProperty("java.io.tmpdir"));
  }

  public static Path baseDirDefault() {
    return Path.of(System.getProperty("user.home"), "piola", "data");
  }

  public static InetAddress multicastGroupDefault() {
    return InetAddress.ofLiteral("224.0.0.1");
  }
}
