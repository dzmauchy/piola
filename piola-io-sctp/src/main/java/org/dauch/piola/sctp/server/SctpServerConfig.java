package org.dauch.piola.sctp.server;

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

import org.dauch.piola.io.annotation.Conf;
import org.dauch.piola.io.annotation.Default;
import org.dauch.piola.io.api.conf.SctpServerConfigIO;
import org.dauch.piola.sctp.CommonConfig;
import org.dauch.piola.io.server.ServerConfig;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Properties;

@Conf
public record SctpServerConfig(
  @Default("0") int id,
  @Default("addressDefault()") InetSocketAddress address,
  @Default("128") int maxStreams,
  @Default("64") int backlog,
  @Default("false") boolean nagle,
  @Default("60") int linger,
  @Default("1 << 20") int rcvBufSize,
  @Default("1 << 20") int sendBufSize,
  @Default("1 << 20") int maxMessageSize,
  @Default("256") int queueSize,
  @Default("512") int bufferCount,
  @Default("0.25f") float freeRatio,
  @Default("bufferDirDefault()") Path bufferDir,
  @Default("baseDirDefault()") Path baseDir,
  @Default("true") boolean sparse
) implements CommonConfig, ServerConfig {

  public static SctpServerConfig fromProperties(String prefix, Properties properties) {
    return SctpServerConfigIO.get(prefix, properties);
  }

  public static SctpServerConfig fromProperties(String prefix) {
    return fromProperties(prefix, System.getProperties());
  }

  public static SctpServerConfig fromProperties() {
    return fromProperties("piola.server");
  }

  public static InetSocketAddress addressDefault() {
    return new InetSocketAddress(InetAddress.ofLiteral("0.0.0.0"), 0);
  }

  public static Path bufferDirDefault() {
    return Path.of(System.getProperty("java.io.tmpdir"));
  }

  public static Path baseDirDefault() {
    return Path.of(System.getProperty("user.home"), "piola", "data");
  }
}
