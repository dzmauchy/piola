package org.dauch.piola.udp.client;

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
import org.dauch.piola.api.conf.UdpClientConfigIO;
import org.dauch.piola.client.ClientConfig;
import org.dauch.piola.udp.ServerClientConfig;

import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.file.Path;
import java.util.Properties;

@Conf
public record UdpClientConfig(
  @Default("\"default\"") String name,
  @Default("INET") StandardProtocolFamily protocolFamily,
  InetSocketAddress address,
  @Default("60") int linger,
  @Default("1 << 20") int rcvBufSize,
  @Default("1 << 20") int sendBufSize,
  @Default("1 << 20") int maxMessageSize,
  @Default("256") int queueSize,
  @Default("128") int bufferCount,
  @Default("0.25f") float freeRatio,
  @Default("bufferDirDefault()") Path bufferDir,
  @Default("255") int multicastTtl,
  @Default("true") boolean multicastLoop,
  @Default("60000") int messageAssemblyTimeout,
  @Default("65536") int maxFragmentSize,
  @Default("1024") int fragmentPayloadSize,
  @Default("bufferCount * 16") int fragmentBufferCount,
  @Default("true") boolean sparse,
  @Default("5") int ackTimeout
) implements ClientConfig, ServerClientConfig {

  public static Path bufferDirDefault() {
    return Path.of(System.getProperty("java.io.tmpdir"));
  }

  public static UdpClientConfig fromProperties(String prefix, Properties properties) {
    return UdpClientConfigIO.get(prefix, properties);
  }

  public static UdpClientConfig fromProperties(String prefix) {
    return UdpClientConfigIO.get(prefix, System.getProperties());
  }

  public static UdpClientConfig fromProperties() {
    return fromProperties("piola.client");
  }
}
