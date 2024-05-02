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
import java.util.*;

import static java.lang.Integer.remainderUnsigned;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public final class ByteBufferIntMap {

  private final Entry[][] entries;

  public ByteBufferIntMap(ByteBuffer[] buffers) {
    var len = buffers.length;
    entries = new Entry[len][];
    var m = HashMap.<Integer, List<Entry>>newHashMap(len);
    for (int i = 0; i < buffers.length; i++) {
      var b = buffers[i];
      m.computeIfAbsent(remainderUnsigned(identityHashCode(b), len), _ -> new LinkedList<>()).addLast(new Entry(i, b));
    }
    m.forEach((i, l) -> entries[i] = l.toArray(Entry[]::new));
  }

  public int get(ByteBuffer buffer) {
    var i = remainderUnsigned(identityHashCode(buffer), entries.length);
    var es = requireNonNull(entries[i], () -> "Not pooled: " + buffer);
    for (var e : es) {
      if (e.buffer == buffer) {
        return e.index;
      }
    }
    throw new NoSuchElementException("Not pooled: " + buffer);
  }

  private record Entry(int index, ByteBuffer buffer) {}
}
