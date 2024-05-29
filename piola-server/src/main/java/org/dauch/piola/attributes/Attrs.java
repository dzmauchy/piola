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
import java.util.Date;
import java.util.function.*;

public abstract sealed class Attrs permits SimpleAttrs, FileAttrs, EmptyAttrs {

  abstract long readRaw(long key) throws NoValueException;
  abstract int size();
  abstract long getKeyByIndex(int index);
  abstract long getValueByIndex(int index);
  public abstract void write(ByteBuffer buffer);

  public final byte getByte(String key, IntSupplier def) {
    try {
      return (byte) readRaw(Id.decode(key));
    } catch (NoValueException _) {
      return (byte) def.getAsInt();
    }
  }

  public final short getShort(String key, IntSupplier def) {
    try {
      return (short) readRaw(Id.decode(key));
    } catch (NoValueException _) {
      return (short) def.getAsInt();
    }
  }

  public final int getInt(String key, IntSupplier def) {
    try {
      return (int) readRaw(Id.decode(key));
    } catch (NoValueException _) {
      return def.getAsInt();
    }
  }

  public final long getLong(String key, LongSupplier def) {
    try {
      return readRaw(Id.decode(key));
    } catch (NoValueException _) {
      return def.getAsLong();
    }
  }

  public final float getFloat(String key, DoubleSupplier def) {
    try {
      return Float.intBitsToFloat((int) readRaw(Id.decode(key)));
    } catch (NoValueException _) {
      return (float) def.getAsDouble();
    }
  }

  public final double getDouble(String key, DoubleSupplier def) {
    try {
      return Double.longBitsToDouble(readRaw(Id.decode(key)));
    } catch (NoValueException _) {
      return def.getAsDouble();
    }
  }

  public final String getString(String key, Supplier<String> def) {
    try {
      return Id.encode(readRaw(Id.decode(key)));
    } catch (NoValueException _) {
      return def.get();
    }
  }

  public final Date getDate(String key, Supplier<Date> def) {
    try {
      return new Date(readRaw(Id.decode(key)));
    } catch (NoValueException _) {
      return def.get();
    }
  }

  @Override
  public final int hashCode() {
    var hash = 7;
    for (int i = 0, s = size(); i < s; i++) {
      hash = hash * 31 + (int) getKeyByIndex(i);
      hash = hash * 31 + (int) getValueByIndex(i);
    }
    return hash;
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj instanceof Attrs that) {
      if (this.size() != that.size()) {
        return false;
      }
      for (int i = 0, s = size(); i < s; i++) {
        if (getKeyByIndex(i) != that.getKeyByIndex(i)) {
          return false;
        }
        if (getValueByIndex(i) != that.getValueByIndex(i)) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public final String toString() {
    return getClass().getSimpleName() + "(" + size() + ")";
  }
}
