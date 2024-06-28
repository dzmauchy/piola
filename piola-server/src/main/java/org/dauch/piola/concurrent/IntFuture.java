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

import java.util.function.IntSupplier;

public final class IntFuture {

  private volatile int result;
  private volatile Throwable throwable;
  private volatile boolean running;

  public IntFuture(IntSupplier supplier) {
    running = true;
    Thread.startVirtualThread(() -> {
      try {
        result = supplier.getAsInt();
      } catch (Throwable e) {
        throwable = e;
      } finally {
        running = false;
      }
    });
  }

  public IntFuture(int value) {
    result = value;
    running = false;
  }

  public int join() {
    while (running) {
      Thread.onSpinWait();
    }
    switch (throwable) {
      case null -> {
        return result;
      }
      case RuntimeException e -> throw e;
      case Error e -> throw e;
      case Throwable e -> throw new IllegalStateException(e);
    }
  }
}
