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
import org.dauch.piola.util.Id;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

public final class SimpleAttrs extends Attrs {

  long[] keys;
  long[] values;

  public SimpleAttrs() {
    this(new long[0], new long[0]);
  }

  SimpleAttrs(long[] keys, long[] values) {
    this.keys = keys;
    this.values = values;
  }

  public SimpleAttrs(ByteBuffer buffer) {
    var count = buffer.getInt();
    keys = new long[count];
    values = new long[count];
    for (int i = 0; i < count; i++) {
      keys[i] = buffer.getLong();
      values[i] = buffer.getLong();
    }
  }

  @Override
  public void write(ByteBuffer buffer) {
    var count = keys.length;
    buffer.putInt(count);
    for (int i = 0; i < count; i++) {
      buffer.putLong(keys[i]).putLong(values[i]);
    }
  }

  @Override
  long readRaw(long key) throws NoValueException {
    var i = Arrays.binarySearch(keys, key);
    if (i >= 0) {
      return values[i];
    } else {
      throw NoValueException.NO_VALUE_EXCEPTION;
    }
  }

  public void putByte(String key, byte v) {
    putRaw(Id.decode(key), v);
  }

  public void putShort(String key, short v) {
    putRaw(Id.decode(key), v);
  }

  public void putInt(String key, int v) {
    putRaw(Id.decode(key), v);
  }

  public void putLong(String key, long v) {
    putRaw(Id.decode(key), v);
  }

  public void putFloat(String key, float v) {
    putRaw(Id.decode(key), Float.floatToRawIntBits(v));
  }

  public void putDouble(String key, double v) {
    putRaw(Id.decode(key), Double.doubleToRawLongBits(v));
  }

  public void putString(String key, String v) {
    putRaw(Id.decode(key), Id.decode(v));
  }

  public void putDate(String key, Date v) {
    putRaw(Id.decode(key), v.getTime());
  }

  private void putRaw(long key, long value) {
    var i = Arrays.binarySearch(keys, key);
    if (i >= 0) {
      values[i] = value;
    } else {
      var ks = keys;
      var vs = values;
      var count = ks.length;
      var nks = new long[count + 1];
      var nvs = new long[count + 1];
      if ((i = -(i + 1)) < count) {
        System.arraycopy(ks, i, nks, i + 1, count - i);
        System.arraycopy(vs, i, nvs, i + 1, count - i);
      }
      if (i > 0) {
        System.arraycopy(ks, 0, nks, 0, i);
        System.arraycopy(vs, 0, nvs, 0, i);
      }
      nks[i] = key;
      nvs[i] = value;
      keys = nks;
      values = nvs;
    }
  }

  @Override
  int size() {
    return keys.length;
  }

  @Override
  long getKeyByIndex(int index) {
    return keys[index];
  }

  @Override
  long getValueByIndex(int index) {
    return values[index];
  }
}
