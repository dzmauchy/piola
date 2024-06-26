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

import org.dauch.piola.api.SerializationContext;
import org.dauch.piola.api.request.Request;
import org.dauch.piola.server.ServerRequest;
import org.dauch.piola.udp.fragment.MsgKey;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public record UdpRq(
  MsgKey key,
  int protocolId,
  int fragmentLength,
  InetSocketAddress address,
  Request<?> request,
  ByteBuffer buffer,
  SerializationContext context
) implements ServerRequest {

  @Override
  public long id() {
    return key.id();
  }

  @Override
  public int stream() {
    return key.stream();
  }
}
