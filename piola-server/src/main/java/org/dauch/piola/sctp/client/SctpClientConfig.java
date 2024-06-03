package org.dauch.piola.sctp.client;

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
import org.dauch.piola.api.conf.SctpClientConfigIO;
import org.dauch.piola.client.ClientConfig;
import org.dauch.piola.sctp.ServerClientConfig;

import java.nio.file.Path;
import java.util.Properties;

@Conf
public record SctpClientConfig(
  @Default("\"default\"") String name,
  @Default("64") int bufferCount,
  @Default("2 << 20") int maxMessageSize,
  @Default("1 << 20") int rcvBufSize,
  @Default("1 << 20") int sendBufSize,
  @Default("false") boolean nagle,
  @Default("60") int linger,
  @Default("0.25f") float freeRatio,
  @Default("128") int maxStreams,
  @Default("bufferDirDefault()") Path bufferDir,
  @Default("true") boolean sparse
) implements ClientConfig, ServerClientConfig {

  public static SctpClientConfig fromProperties(String prefix, Properties properties) {
    return SctpClientConfigIO.get(prefix, properties);
  }

  public static SctpClientConfig fromProperties(String prefix) {
    return fromProperties(prefix, System.getProperties());
  }

  public static Path bufferDirDefault() {
    return Path.of(System.getProperty("java.io.tmpdir"));
  }
}
