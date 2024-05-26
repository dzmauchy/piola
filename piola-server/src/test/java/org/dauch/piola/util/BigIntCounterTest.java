package org.dauch.piola.util;

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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.util.BigIntCounter.THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BigIntCounterTest {

  @Test
  void singleThreadedAdd() {
    // given
    var counter = new BigIntCounter(THRESHOLD - 10_000L, ZERO);
    // when
    counter.add(10_001);
    // then
    assertEquals(BigInteger.valueOf(THRESHOLD).add(ONE), counter.get());
  }

  @Test
  void singleThreadedIncrement() {
    // given
    var counter = new BigIntCounter(THRESHOLD, ZERO);
    // when
    counter.increment();
    // then
    assertEquals(BigInteger.valueOf(THRESHOLD).add(ONE), counter.get());
  }

  @Test
  void multiThreadedAdd() throws Exception {
    // given
    var counter = new BigIntCounter(THRESHOLD - 100_000L, ZERO);
    var expected = new AtomicLong(THRESHOLD - 100_000L);
    // when
    var threads = IntStream.range(0, 10)
      .mapToObj(_ -> new Thread(() -> {
        parkNanos(1_000_000L);
        while (!Thread.currentThread().isInterrupted()) {
          counter.add(10);
          expected.getAndAdd(10);
        }
      }))
      .peek(Thread::start)
      .toArray(Thread[]::new);
    while (expected.get() < THRESHOLD + 100_000L) {
      parkNanos(1_000_000L);
    }
    for (var thread : threads) {
      thread.interrupt();
      thread.join();
    }
    // then
    assertEquals(expected.get(), counter.get().longValue());
  }

  @Test
  void multiThreadedIncrement() throws Exception {
    // given
    var counter = new BigIntCounter(THRESHOLD - 10_000L, ZERO);
    var expected = new AtomicLong(THRESHOLD - 10_000L);
    // when
    var threads = IntStream.range(0, 10)
      .mapToObj(_ -> new Thread(() -> {
        parkNanos(1_000_000L);
        while (!Thread.currentThread().isInterrupted()) {
          counter.increment();
          expected.getAndIncrement();
        }
      }))
      .peek(Thread::start)
      .toArray(Thread[]::new);
    while (expected.get() < THRESHOLD + 10_000L) {
      parkNanos(1_000_000L);
    }
    for (var thread : threads) {
      thread.interrupt();
      thread.join();
    }
    // then
    assertEquals(expected.get(), counter.get().longValue());
  }
}
