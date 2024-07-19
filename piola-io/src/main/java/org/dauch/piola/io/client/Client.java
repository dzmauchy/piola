package org.dauch.piola.io.client;

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

import org.dauch.piola.io.api.request.Request;
import org.dauch.piola.io.api.response.Response;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Piola client interface
 */
public interface Client extends ClientMXBean, AutoCloseable {

  /**
   * Sends a request to servers
   * @param request Request
   * @param payload Payload (or null if absent)
   * @param stream A stream index to send the request to
   * @param addresses Addresses to send to
   * @return Server responses wrapper
   * @param <RQ> Request type
   * @param <RS> Response type
   */
  <RQ extends Request<RS>, RS extends Response> Responses<RS> send(
    RQ request,
    ByteBuffer payload,
    int stream,
    InetSocketAddress... addresses
  );
}
