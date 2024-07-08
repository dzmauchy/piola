package org.dauch.piola.client;

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

import org.dauch.piola.api.response.Response;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface Responses<R extends Response> {
  ClientResponse<? extends R> poll(long time, TimeUnit timeUnit);
  ClientResponse<? extends R> take();
  ClientResponse<? extends R> poll();
  ClientResponse<? extends R> peek();
  boolean fetch(Predicate<ClientResponse<? extends R>> consumer);
  boolean poll(Consumer<ClientResponse<? extends R>> consumer);
  boolean isEmpty();
  Throwable error(InetSocketAddress address);
  boolean hasErrors();
}
