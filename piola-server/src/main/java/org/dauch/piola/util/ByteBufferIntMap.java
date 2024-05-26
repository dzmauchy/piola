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

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import static java.lang.Integer.remainderUnsigned;
import static java.lang.System.identityHashCode;

public final class ByteBufferIntMap {

  private final Entry[] entries;

  public ByteBufferIntMap(ByteBuffer[] buffers) {
    entries = new Entry[buffers.length];
    for (int i = 0; i < buffers.length; i++) {
      var b = buffers[i];
      var index = remainderUnsigned(identityHashCode(b), buffers.length);
      entries[index] = new Entry(i, b, entries[index]);
    }
  }

  public int get(ByteBuffer buffer) {
    var i = remainderUnsigned(identityHashCode(buffer), entries.length);
    for (var e = entries[i]; e != null; e = e.prev) {
      if (e.buffer == buffer) {
        return e.index;
      }
    }
    throw new NoSuchElementException("Not pooled: " + buffer);
  }

  private record Entry(int index, ByteBuffer buffer, Entry prev) {}
}
