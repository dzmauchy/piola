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

import java.util.Arrays;
import java.util.function.IntFunction;

public final class DrainQueue<E> {

  private final E[] queue;
  private int putIndex;

  public DrainQueue(int capacity, IntFunction<E[]> generator) {
    queue = generator.apply(capacity);
  }

  public synchronized void put(final E element) throws InterruptedException {
    while (putIndex == queue.length)
      wait(0L);
    queue[putIndex++] = element;
  }

  public synchronized int drain(final E[] array) {
    var i = putIndex;
    if (i == 0) {
      return 0;
    } else {
      var q = queue;
      System.arraycopy(q, 0, array, 0, i);
      Arrays.fill(q, null);
      putIndex = 0;
      notifyAll();
      return i;
    }
  }

  public synchronized boolean isEmpty() {
    return putIndex == 0;
  }

  public synchronized boolean nonEmpty() {
    return putIndex != 0;
  }

  public int capacity() {
    return queue.length;
  }
}
