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

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Consumer;

public final class Closeables implements Iterable<AutoCloseable>, Closeable {

  private Elem elements;

  public <T extends AutoCloseable> T add(T closeable) {
    elements = new Elem(closeable, elements);
    return closeable;
  }

  @Override
  public Iterator<AutoCloseable> iterator() {
    return new Iterator<>() {

      private Elem e = elements;

      @Override
      public boolean hasNext() {
        return e != null;
      }

      @Override
      public AutoCloseable next() {
        var r = e.closeable;
        e = e.prev;
        return r;
      }
    };
  }

  @Override
  public void forEach(Consumer<? super AutoCloseable> action) {
    for (var e = elements; e != null; e = e.prev) {
      action.accept(e.closeable);
    }
  }

  public <E extends Throwable> E closeAndWrap(E exception) {
    for (var e = elements; e != null; elements = e.prev, e = elements) {
      try {
        e.closeable.close();
      } catch (Throwable x) {
        exception.addSuppressed(x);
      }
    }
    return exception;
  }

  @Override
  public void close() {
    var c = new IllegalStateException("Close error");
    for (var e = elements; e != null; e = e.prev) {
      try {
        e.closeable.close();
      } catch (Throwable x) {
        c.addSuppressed(x);
      }
    }
    if (c.getSuppressed().length > 0) {
      throw c;
    }
  }

  private record Elem(AutoCloseable closeable, Elem prev) {}
}
