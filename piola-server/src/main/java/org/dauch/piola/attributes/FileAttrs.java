package org.dauch.piola.attributes;

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

import org.dauch.piola.exception.NoValueException;

import java.nio.ByteBuffer;

public final class FileAttrs extends Attrs {

  final ByteBuffer buffer;
  int size;

  public FileAttrs(ByteBuffer buffer) {
    this.buffer = buffer;
    this.size = size(buffer);
  }

  private static int size(ByteBuffer buffer) {
    for (int i = 0, l = buffer.capacity() >>> 4; i < l; i++) {
      var k = buffer.getLong(i << 4);
      if (k == 0L) {
        return i;
      }
    }
    return buffer.capacity() >>> 4;
  }

  @Override
  int size() {
    return size;
  }

  @Override
  long getKeyByIndex(int index) {
    return buffer.getLong(index << 4);
  }

  @Override
  long getValueByIndex(int index) {
    return buffer.getLong((index << 4) + 8);
  }

  public void update(SimpleAttrs attrs) {
    var ks = attrs.keys;
    var vs = attrs.values;
    if (size == 0) {
      size = ks.length;
      buffer.position(0).limit(size << 4);
      for (int i = 0, l = ks.length; i < l; i++) {
        buffer.putLong(ks[i]).putLong(vs[i]);
      }
      return;
    }
    for (int i = 0, l = ks.length; i < l; i++) {
      put(ks[i], vs[i]);
    }
  }

  public void put(long key, long value) {
    var i = binarySearch(key);
    if (i >= 0) {
      buffer.putLong((i << 4) + 8, value);
    } else {
      i = -(i + 1);
      for (int j = size; j > i; j--) {
        buffer.putLong(j << 4, buffer.getLong((j - 1) << 4));
        buffer.putLong((j << 4) + 8, buffer.getLong(((j - 1) << 4) + 8));
      }
      buffer.putLong(i << 4, key);
      buffer.putLong((i << 4) + 8, value);
      size++;
    }
  }

  @Override
  long readRaw(long key) throws NoValueException {
    int i = binarySearch(key);
    if (i >= 0) {
      return buffer.getLong((i << 4) + 8);
    } else {
      throw NoValueException.NO_VALUE_EXCEPTION;
    }
  }

  private int binarySearch(long key) {
    int l = 0, h = size - 1;
    while (l <= h) {
      int m = (l + h) >>> 1;
      long v = buffer.getLong(m << 4);
      if (key < v) l = m + 1;
      else if (key > v) h = m - 1;
      else return m;
    }
    return -(l + 1);
  }

  @Override
  public void write(ByteBuffer buffer) {
    buffer.putInt(size).put(this.buffer.position(0).limit(size << 4));
  }
}
