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

import java.util.concurrent.*;
import java.util.function.BiFunction;

public final class SimpleResponses<R extends Response> implements Responses<R> {

  public final long id;
  private final LinkedTransferQueue<ClientResponse<? extends R>> queue;

  @SuppressWarnings("unchecked")
  public SimpleResponses(long id, LinkedTransferQueue<ClientResponse<? extends Response>> queue) {
    this.id = id;
    this.queue = (LinkedTransferQueue<ClientResponse<? extends R>>) (LinkedTransferQueue<?>) queue;
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
  public <A> CompletableFuture<A> poll(A initial, BiFunction<A, ClientResponse<? extends R>, A> op) {
    final class F<T> extends CompletableFuture<T> {

      private volatile SimpleResponses<R> responses;

      private F(SimpleResponses<R> responses) {
        this.responses = responses;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        responses = null;
        return super.cancel(mayInterruptIfRunning);
      }

      @Override
      public <U> CompletableFuture<U> newIncompleteFuture() {
        return new F<>(responses);
      }
    }
    var f = new F<A>(this);
    Thread.startVirtualThread(() -> {
      var a = initial;
      try {
        while (!f.isCancelled()) {
          var e = queue.poll(1L, TimeUnit.SECONDS);
          if (e == null) continue;
          var newA = op.apply(a, e);
          if (newA == null) {
            f.complete(a);
          } else {
            a = newA;
          }
        }
      } catch (Throwable e) {
        f.completeExceptionally(e);
      }
    });
    return f;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }
}
