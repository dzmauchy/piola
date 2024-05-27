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
import java.util.IdentityHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;

import static java.lang.System.nanoTime;
import static java.util.concurrent.Future.State.*;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public final class SimpleResponses<R extends Response> implements Responses<R> {

  public final long id;
  private final LinkedTransferQueue<ClientResponse<? extends R>> queue;
  final IdentityHashMap<InetSocketAddress, Throwable> errors = new IdentityHashMap<>();

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
  public <A> Future<A> poll(A initial, BiFunction<A, ClientResponse<? extends R>, A> op) {
    var f = new F<A>(this);
    Thread.startVirtualThread(() -> {
      var a = initial;
      try {
        while (f.cancelled == 0) {
          var e = queue.poll();
          if (e == null) {
            parkNanos(1_000_000L);
            continue;
          }
          var newA = op.apply(a, e);
          if (newA == null) {
            f.result = a;
            f.finish();
            break;
          } else {
            a = newA;
          }
        }
      } catch (Throwable e) {
        f.error = e;
        f.finish();
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

  @Override
  public Throwable error(InetSocketAddress address) {
    synchronized (errors) {
      return errors.get(address);
    }
  }

  @Override
  public boolean hasErrors() {
    synchronized (errors) {
      return !errors.isEmpty();
    }
  }

  private static final class F<T> implements Future<T> {

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<F> U = newUpdater(F.class, "cancelled");

    @SuppressWarnings("unused")
    private volatile SimpleResponses<?> responses;
    private volatile int cancelled;
    private volatile boolean finished;
    private volatile T result;
    private volatile Throwable error;

    private final Object lock = new Object();

    public F(SimpleResponses<?> responses) {
      this.responses = responses;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (U.compareAndSet(this, 0, 1)) {
        finish();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean isCancelled() {
      return cancelled > 0;
    }

    @Override
    public boolean isDone() {
      return finished;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      synchronized (lock) {
        while (!finished) {
          lock.wait(0L);
        }
      }
      if (cancelled > 0)
        throw new CancellationException();
      if (error != null)
        throw new ExecutionException(error);
      return result;
    }

    @Override
    public State state() {
      return finished ? (cancelled > 0 ? CANCELLED : (error != null ? FAILED : SUCCESS)) : RUNNING;
    }

    @Override
    public T resultNow() {
      return switch (state()) {
        case CANCELLED -> throw new IllegalStateException("Task was cancelled");
        case RUNNING -> throw new IllegalStateException("Task is still running");
        case FAILED -> throw new IllegalStateException("Task was failed", error);
        case SUCCESS -> result;
      };
    }

    @Override
    public Throwable exceptionNow() {
      return switch (state()) {
        case CANCELLED -> throw new IllegalStateException("Task was cancelled");
        case RUNNING -> throw new IllegalStateException("Task is still running");
        case FAILED -> error;
        case SUCCESS -> throw new IllegalStateException("Task was finished successfully");
      };
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      var timeoutMillis = unit.toMillis(timeout);
      var millis = Math.max(1L, unit.toMillis(timeout));
      synchronized (lock) {
        for (long t = timeoutMillis, start = nanoTime(); !finished; start = nanoTime()) {
          if (t < 0L) throw new TimeoutException();
          lock.wait(millis);
          t -= Math.max(1L, (nanoTime() - start) / 1_000_000L);
        }
      }
      if (cancelled > 0)
        throw new CancellationException();
      if (error != null)
        throw new ExecutionException(error);
      return result;
    }

    private void finish() {
      finished = true;
      responses = null;
      synchronized (lock) {
        lock.notifyAll();
      }
    }
  }
}
