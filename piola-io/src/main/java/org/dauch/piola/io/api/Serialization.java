package org.dauch.piola.io.api;

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

import org.dauch.piola.io.api.index.IndexType;
import org.dauch.piola.io.api.index.IndexValue;
import org.dauch.piola.io.exception.ExceptionData;
import org.dauch.piola.util.Id;

import java.nio.ByteBuffer;
import java.time.*;

public final class Serialization {

  private Serialization() {
  }

  public static String read(ByteBuffer buffer, String def) {
    var data = new char[buffer.getInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = buffer.getChar();
    }
    return new String(data);
  }

  public static void write(ByteBuffer buffer, String s) {
    var l = s.length();
    buffer.putInt(l);
    for (int i = 0; i < l; i++) {
      buffer.putChar(s.charAt(i));
    }
  }

  public static LocalDateTime read(ByteBuffer buffer, LocalDateTime def) {
    var v = buffer.getLong();
    return LocalDateTime.ofEpochSecond(v / 1000L, (int) ((v % 1000)) * 1_000_000, ZoneOffset.UTC);
  }

  public void write(ByteBuffer buffer, LocalDateTime dateTime) {
    var v = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    buffer.putLong(v);
  }

  public static Duration read(ByteBuffer buffer, Duration def) {
    var v = buffer.getLong();
    return Duration.ofNanos(v);
  }

  public static void write(ByteBuffer buffer, Duration duration) {
    buffer.putLong(duration.toNanos());
  }

  public static int read(ByteBuffer buffer, int def) {
    return buffer.getInt();
  }

  public static void write(ByteBuffer buffer, int value) {
    buffer.putInt(value);
  }

  public static long read(ByteBuffer buffer, long def) {
    return buffer.getLong();
  }

  public static void write(ByteBuffer buffer, long value) {
    buffer.putLong(value);
  }

  public static boolean read(ByteBuffer buffer, boolean value) {
    return buffer.get() != 0;
  }

  public static void write(ByteBuffer buffer, boolean value) {
    buffer.put(value ? (byte) 1 : 0);
  }

  public static ExceptionData read(ByteBuffer buffer, ExceptionData value) {
    return buffer.get() == 0 ? null : ExceptionData.read(buffer);
  }

  public static void write(ByteBuffer buffer, ExceptionData value) {
    if (value == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      value.write(buffer);
    }
  }

  public static String[] read(ByteBuffer buffer, String[] value) {
    var array = new String[buffer.getInt()];
    for (int i = 0; i < array.length; i++) {
      array[i] = read(buffer, "");
    }
    return array;
  }

  public static void write(ByteBuffer buffer, String[] value) {
    buffer.putInt(value.length);
    for (var v : value) write(buffer, v);
  }

  public static void write(ByteBuffer buffer, Id id) {
    buffer.putLong(id.value());
  }

  public static Id read(ByteBuffer buffer, Id value) {
    return new Id(buffer.getLong());
  }

  public static void write(ByteBuffer buffer, Instant instant) {
    buffer.putLong(instant.getEpochSecond());
    buffer.putInt(instant.getNano());
  }

  public static Instant read(ByteBuffer buffer, Instant value) {
    var seconds = buffer.getLong();
    var nanos = buffer.getInt();
    return Instant.ofEpochSecond(seconds, nanos);
  }

  public static void write(ByteBuffer buffer, IndexValue[] value) {
    if (value == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) value.length);
      for (var v : value) {
        buffer.putLong(v.key());
        buffer.putLong(v.value());
        buffer.put((byte) v.type().ordinal());
      }
    }
  }

  public static IndexValue[] read(ByteBuffer buffer, IndexValue[] value) {
    var values = new IndexValue[buffer.get()];
    for (int i = 0; i < values.length; i++) {
      var k = buffer.getLong();
      var v = buffer.getLong();
      var t = IndexType.byId(buffer.get());
      values[i] = new IndexValue(k, v, t);
    }
    return values;
  }
}
