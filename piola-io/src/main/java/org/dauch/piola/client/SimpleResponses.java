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
import org.dauch.piola.util.Addr;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

final class SimpleResponses<R extends Response> implements Responses<R>, AutoCloseable {

  final long id;
  private final LinkedTransferQueue<ClientResponse<? extends R>> queue;
  private final Runnable close;
  private final ConcurrentSkipListMap<Addr, Throwable> errors = new ConcurrentSkipListMap<>();

  @SuppressWarnings("unchecked")
  SimpleResponses(long id, LinkedTransferQueue<ClientResponse<? extends Response>> queue, Runnable close) {
    this.id = id;
    this.queue = (LinkedTransferQueue<ClientResponse<? extends R>>) (LinkedTransferQueue<?>) queue;
    this.close = close;
  }

  @Override
  public ClientResponse<? extends R> poll(long time, TimeUnit timeUnit) {
    try {
      return queue.poll(time, timeUnit);
    } catch (InterruptedException e) {
      var ce = new CancellationException("Interrupted");
      ce.initCause(e);
      throw ce;
    }
  }

  @Override
  public ClientResponse<? extends R> take() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      var ce = new CancellationException("Interrupted");
      ce.initCause(e);
      throw ce;
    }
  }

  @Override
  public ClientResponse<? extends R> poll() {
    return queue.poll();
  }

  @Override
  public ClientResponse<? extends R> peek() {
    return queue.peek();
  }

  @Override
  public boolean poll(Consumer<ClientResponse<? extends R>> consumer) {
    return queue.removeIf(e -> {
      consumer.accept(e);
      return true;
    });
  }

  @Override
  public boolean fetch(Predicate<ClientResponse<? extends R>> consumer) {
    return queue.removeIf(consumer);
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public Throwable error(InetSocketAddress address) {
    return errors.get(new Addr(address));
  }

  @Override
  public boolean hasErrors() {
    return !errors.isEmpty();
  }

  void putError(InetSocketAddress address, Throwable error) {
    errors.put(new Addr(address), error);
  }

  @Override
  public void close() {
    close.run();
  }
}
