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

import java.util.LinkedHashMap;

import static java.lang.System.Logger.Level.INFO;

public abstract class CompositeCloseable implements AutoCloseable {

  protected final System.Logger logger;
  private final LinkedHashMap<String, AutoCloseable> killers = new LinkedHashMap<>(8, 0.1f);

  protected CompositeCloseable(String loggerName) {
    logger = System.getLogger(loggerName);
  }

  protected <C extends AutoCloseable> C $(String name, C closeable) {
    var old = killers.putLast(name, closeable);
    if (old != null && old != closeable) {
      throw new IllegalStateException("Duplicated closeable " + name);
    }
    return closeable;
  }

  protected <E extends Throwable> E constructorException(E exception) {
    try {
      close();
    } catch (Throwable e) {
      exception.addSuppressed(e);
    }
    return exception;
  }

  @Override
  public void close() {
    var exception = new IllegalStateException("Close exception");
    for (var it = killers.reversed().entrySet().iterator(); it.hasNext(); ) {
      var entry = it.next();
      var name = entry.getKey();
      var closeable = entry.getValue();
      try {
        logger.log(INFO, () -> "Closing " + name);
        closeable.close();
        logger.log(INFO, () -> "Closed " + name);
      } catch (Throwable e) {
        exception.addSuppressed(new IllegalStateException("Unable to close " + name, e));
      } finally {
        it.remove();
      }
    }
    if (exception.getSuppressed().length > 0) {
      throw exception;
    }
  }
}
