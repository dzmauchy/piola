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

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public final class BigIntCounter {

  private static final AtomicLongFieldUpdater<BigIntCounter> COUNTER = newUpdater(BigIntCounter.class, "counter");
  static final long THRESHOLD = 1L << 62;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile long counter;
  private BigInteger base;

  public BigIntCounter() {
    this(0L, BigInteger.ZERO);
  }

  BigIntCounter(long initial, BigInteger base) {
    this.counter = initial;
    this.base = base;
  }

  /**
   * Adds a value to the counter
   *
   * @param value A value to add
   */
  public void add(int value) {
    if (COUNTER.addAndGet(this, value) > THRESHOLD) {
      lock.writeLock().lock();
      try {
        var old = COUNTER.getAndSet(this, 0L);
        base = base.add(BigInteger.valueOf(old));
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  /**
   * Increments the counter
   */
  public void increment() {
    if (COUNTER.incrementAndGet(this) > THRESHOLD) {
      lock.writeLock().lock();
      try {
        var old = COUNTER.getAndSet(this, 0L);
        base = base.add(BigInteger.valueOf(old));
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public BigInteger get() {
    lock.readLock().lock();
    try {
      return base.add(BigInteger.valueOf(counter));
    } finally {
      lock.readLock().unlock();
    }
  }
}
