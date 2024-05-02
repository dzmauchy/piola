package org.dauch.piola.test;

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

import com.sun.management.ThreadMXBean;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public interface MemoryConsumption {

  int COUNT = 1000;

  static long memoryConsumption(Supplier<?> supplier) {
    var threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    var size = new AtomicLong();
    var hash = new AtomicLong();
    var thread = new Thread(() -> {
      var data = new Object[COUNT];
      for (var i = 0; i < 5; i++) {
        System.gc();
      }
      var startSize = threadBean.getThreadAllocatedBytes(Thread.currentThread().threadId());
      for (var i = 0; i < data.length; i++) {
        data[i] = supplier.get();
      }
      for (var i = 0; i < 5; i++) {
        System.gc();
      }
      var endSize = threadBean.getThreadAllocatedBytes(Thread.currentThread().threadId());
      hash.set(Arrays.hashCode(data));
      size.set((endSize - startSize) / COUNT);
    });
    assert hash.get() != Long.MAX_VALUE;
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException _) {
      throw new CancellationException();
    }
    return size.get();
  }
}
