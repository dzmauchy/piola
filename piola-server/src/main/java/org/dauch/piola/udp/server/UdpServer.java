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

import org.dauch.piola.server.AbstractServer;
import org.dauch.piola.udp.UdpUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.stream.Stream;

public final class UdpServer extends AbstractServer<UdpRq, UdpRs> {

  private final InetAddress address;
  private final NetworkInterface networkInterface;
  private final DatagramChannel channel;
  private final int port;

  public UdpServer(UdpServerConfig config) {
    super(config, UdpRq[]::new, UdpRs[]::new);
    try {
      address = config.address().getAddress();
      networkInterface = config.multicastNetworkInterface();
      channel = $("channel", DatagramChannel.open(config.protocolFamily()));
      UdpUtils.configure(channel, config);
      channel.bind(config.address());
      port = ((InetSocketAddress) channel.getLocalAddress()).getPort();
      var membershipKey = channel.join(config.multicastGroup(), networkInterface);
      startThreads();
      $("membership", membershipKey::drop);
    } catch (Throwable e) {
      throw initException(new IllegalStateException("Unable to start server " + config.id(), e));
    }
  }

  @Override
  public Stream<InetSocketAddress> addresses() {
    try {
      if (networkInterface == null) {
        if (address.isAnyLocalAddress()) {
          return NetworkInterface.networkInterfaces()
            .flatMap(NetworkInterface::inetAddresses)
            .map(a -> new InetSocketAddress(a, port));
        } else {
          return Stream.of(new InetSocketAddress(address, port));
        }
      } else {
        return networkInterface.inetAddresses().map(a -> new InetSocketAddress(a, port));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public InetSocketAddress address(InetAddress address) {
    return new InetSocketAddress(address, port);
  }

  @Override
  public InetSocketAddress address(String host) {
    return new InetSocketAddress(host, port);
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  protected void doInMainLoop(ByteBuffer buf) throws Exception {
  }

  @Override
  protected void doSendShutdownSequence() throws Exception {
  }

  @Override
  protected void requestLoop() {
  }

  @Override
  protected void responseLoop() {
  }
}
