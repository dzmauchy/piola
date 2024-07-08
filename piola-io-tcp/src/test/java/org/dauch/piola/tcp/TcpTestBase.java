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

import org.dauch.piola.tcp.client.TcpClient;
import org.dauch.piola.tcp.client.TcpClientConfig;
import org.dauch.piola.tcp.server.TcpServer;
import org.dauch.piola.tcp.server.TcpServerConfig;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Properties;

import static java.lang.System.Logger.Level.INFO;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 20L, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public abstract class TcpTestBase {

  protected final System.Logger log = System.getLogger(getClass().getName());

  protected TcpServer server;
  protected TcpClient client;
  protected InetSocketAddress address;

  public TcpServer getServer() {
    return server;
  }

  public TcpClient getClient() {
    return client;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  @BeforeEach
  protected void initServer(@TempDir Path baseDir, @TempDir Path bufferDir) {
    var props = new Properties();
    props.setProperty("test.baseDir", baseDir.toString());
    props.setProperty("test.bufferDir", bufferDir.toString());
    props.setProperty("test.bufferCount", "4");
    props.setProperty("test.maxMessageSize", "1000000");
    server = new TcpServer(TcpServerConfig.fromProperties("test", props));
    address = server.address("127.0.0.1");
  }

  @BeforeEach
  protected void initClient(@TempDir Path clientBufferDir) {
    var props = new Properties();
    props.setProperty("test.bufferDir", clientBufferDir.toString());
    props.setProperty("test.bufferCount", "4");
    props.setProperty("test.maxMessageSize", "1000000");
    client = new TcpClient(TcpClientConfig.fromProperties("test", props));
  }

  @AfterEach
  protected void closeClientAndServer() {
    try (var _ = client; var _ = server) {
      log.log(INFO, "Closing");
    } finally {
      client = null;
      server = null;
    }
  }
}
