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

import java.util.function.Supplier;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public interface Threads {

  static void close(Thread thread, Supplier<?> killer, System.Logger logger) {
    for (long t = System.nanoTime(), c = 0L; ; c++) {
      var result = killer.get();
      try {
        thread.join(1000L);
      } catch (InterruptedException _) {
        logger.log(WARNING, () -> "Interrupted while waiting for thread to finish");
      }
      if (!thread.isAlive()) {
        logger.log(INFO, () -> thread.getName() + " finished");
        break;
      }
      if (System.nanoTime() - t > 10_000_000_000L) {
        var count = c + 1;
        logger.log(INFO, () -> "Sent a shutdown sequence to " + result + " " + count + " times");
        t = System.nanoTime();
      }
    }
  }
}
