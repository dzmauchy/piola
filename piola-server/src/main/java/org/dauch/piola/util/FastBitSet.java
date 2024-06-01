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
import java.util.Arrays;

public final class FastBitSet {

  private final long[] words;

  public FastBitSet(int size) {
    if (size <= 0)
      throw new IllegalArgumentException("size must be greater than zero");
    else if (size > Character.MAX_VALUE)
      throw new IllegalArgumentException("size must be less than or equal to " + Character.MAX_VALUE);
    this.words = new long[(size >>> 6) + (size % Long.SIZE == 0 ? 0 : 1)];
  }

  public FastBitSet(ByteBuffer buf) {
    this.words = new long[buf.getChar()];
    for (int i = 0; i < words.length; i++) {
      this.words[i] = buf.getLong();
    }
  }

  public void write(ByteBuffer buf) {
    buf.putChar((char) words.length);
    for (var w : words) {
      buf.putLong(w);
    }
  }

  public void set(int index) {
    words[index >>> 6] |= (1L << index);
  }

  public void clear(int index) {
    words[index >>> 6] &= ~(1L << index);
  }

  public void clear() {
    Arrays.fill(words, 0L);
  }

  public int cardinality() {
    int sum = 0;
    for (var w : words) {
      sum += Long.bitCount(w);
    }
    return sum;
  }

  public boolean isEmpty() {
    for (var w : words) {
      if (w != 0) return false;
    }
    return true;
  }

  public boolean nonEmpty() {
    for (var w : words) {
      if (w != 0L) return true;
    }
    return false;
  }

  public int nextSetBit(int index) {
    var u = index >>> 6;
    if (u >= words.length) return -1;
    var word = words[u] & (-1L << index);
    while (true) {
      if (word != 0L) return (u << 6) + Long.numberOfTrailingZeros(word);
      if (++u == words.length) return -1;
      word = words[u];
    }
  }

  public int trySetFirstClearBit(int size) {
    var u = 0;
    var word = ~words[u];
    while (true) {
      if (word != 0L) {
        var bit = (u << 6) + Long.numberOfTrailingZeros(word);
        if (bit < size) {
          words[u] |= (1L << bit);
          return bit;
        } else {
          return -1;
        }
      }
      if (++u == words.length) return -1;
      word = ~words[u];
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(words);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FastBitSet s && Arrays.equals(words, s.words);
  }

  @Override
  public String toString() {
    var b = new StringBuilder("{");
    var i = nextSetBit(0);
    while (i >= 0) {
      b.append(i);
      i = nextSetBit(i + 1);
      if (i >= 0) b.append(",");
    }
    return b.append('}').toString();
  }
}
