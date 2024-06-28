package org.dauch.piola.concurrent;

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

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

public interface Tasks {

  static void forkAndJoin(Runnable t1, Runnable t2) {
    var e1 = new AtomicReference<Throwable>();
    var e2 = new AtomicReference<Throwable>();
    var thread1 = Thread.startVirtualThread(() -> {
      try {
        t1.run();
      } catch (Throwable e) {
        e1.set(e);
      }
    });
    var thread2 = Thread.startVirtualThread(() -> {
      try {
        t2.run();
      } catch (Throwable e) {
        e2.set(e);
      }
    });
    try {
      thread1.join();
      thread2.join();
      switch (combine(e1.get(), e2.get())) {
        case null -> {}
        case RuntimeException e -> throw e;
        case Error e -> throw e;
        case Throwable e -> throw new IllegalStateException(e);
      }
    } catch (InterruptedException _) {
      throw new CancellationException();
    }
  }

  private static Throwable combine(Throwable e1, Throwable e2) {
    if (e1 != null) {
      if (e2 != null) {
        e1.addSuppressed(e2);
      }
      return e1;
    } else {
      return e2;
    }
  }
}
