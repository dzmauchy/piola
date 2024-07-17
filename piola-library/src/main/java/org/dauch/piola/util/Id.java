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

import java.math.BigInteger;
import java.time.*;
import java.util.*;

import static java.math.BigInteger.ONE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

public record Id(long value) {

  public Id(Instant instant) {
    this(instant.toEpochMilli());
  }

  public Id(LocalDateTime dateTime) {
    this(dateTime.toInstant(ZoneOffset.UTC));
  }

  public Id(ZonedDateTime dateTime) {
    this(dateTime.toInstant());
  }

  public Id(OffsetDateTime dateTime) {
    this(dateTime.toInstant());
  }

  public Id(LocalDate date) {
    this(date.toEpochDay());
  }

  public Id(LocalTime time) {
    this(time.toNanoOfDay());
  }

  public Id(OffsetTime time) {
    this(time.toLocalTime().toNanoOfDay());
  }

  public Id(ZoneOffset offset) {
    this(offset.getTotalSeconds());
  }

  public Id(Duration duration) {
    this(duration.toNanos());
  }

  public Id(Date date) {
    this(date.getTime());
  }

  public Id(String id) {
    this(decode(id));
  }

  public Id(Currency currency) {
    this(currency.getCurrencyCode());
  }

  public Id(Locale locale) {
    this(locale.toLanguageTag().replace('-', '_'));
  }

  public Id(Enum<?> id) {
    this(id.ordinal());
  }

  public Id(float value) {
    this(Float.floatToRawIntBits(value));
  }

  public Id(double value) {
    this(Double.doubleToRawLongBits(value));
  }

  public String asString() {
    return encode(value);
  }

  public Instant asInstant() {
    return Instant.ofEpochMilli(value);
  }

  public LocalDate asLocalDate() {
    return LocalDate.ofEpochDay(value);
  }

  public LocalTime asLocalTime() {
    return LocalTime.ofNanoOfDay(value);
  }

  public BigInteger asUnsigned() {
    var v = BigInteger.valueOf(value);
    return value >= 0L ? v : v.add(ONE.shiftLeft(64));
  }

  public int asInt() {
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      return (int) value;
    } else {
      throw new ArithmeticException("Value out of range: " + value);
    }
  }

  public short asShort() {
    if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      return (short) value;
    } else {
      throw new ArithmeticException("Value out of range: " + value);
    }
  }

  public char asChar() {
    if (value >= Character.MIN_VALUE && value <= Character.MAX_VALUE) {
      return (char) value;
    } else {
      throw new ArithmeticException("Value is out of range: " + value);
    }
  }

  public byte asByte() {
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    } else {
      throw new ArithmeticException("Value is out of range: " + value);
    }
  }

  public float asFloat() {
    return Float.intBitsToFloat((int) value);
  }

  public double asDouble() {
    return Double.longBitsToDouble(value);
  }

  @Override
  public String toString() {
    return Long.toString(value);
  }

  public static long decode(String key) {
    if (key == null)
      throw new NullPointerException("Null key");
    if (key.isEmpty())
      throw new IllegalArgumentException("Empty key");
    int start = 0, len = key.length();
    while (start < len && key.charAt(start) == '_')
      start++;
    if (start == len)
      return 0L;
    long v = 0L, a = 1L;
    for (int i = len - 1; i >= start; i--) {
      var c = switch (key.charAt(i)) {
        case '_' -> 0x00;
        case '0' -> 0x01;
        case '1' -> 0x02;
        case '2' -> 0x03;
        case '3' -> 0x04;
        case '4' -> 0x05;
        case '5' -> 0x06;
        case '6' -> 0x07;
        case '7' -> 0x08;
        case '8' -> 0x09;
        case '9' -> 0x0A;
        case 'a' -> 0x0B;
        case 'b' -> 0x0C;
        case 'c' -> 0x0D;
        case 'd' -> 0x0E;
        case 'e' -> 0x0F;
        case 'f' -> 0x10;
        case 'g' -> 0x11;
        case 'h' -> 0x12;
        case 'i' -> 0x13;
        case 'j' -> 0x14;
        case 'k' -> 0x15;
        case 'l' -> 0x16;
        case 'm' -> 0x17;
        case 'n' -> 0x18;
        case 'o' -> 0x19;
        case 'p' -> 0x1A;
        case 'q' -> 0x1B;
        case 'r' -> 0x1C;
        case 's' -> 0x1D;
        case 't' -> 0x1E;
        case 'u' -> 0x1F;
        case 'v' -> 0x20;
        case 'w' -> 0x21;
        case 'x' -> 0x22;
        case 'y' -> 0x23;
        case 'z' -> 0x24;
        default -> throw new IllegalArgumentException("Invalid key: " + key);
      };
      var d = a * c;
      if (Long.divideUnsigned(d, a) != c) {
        throw new IllegalArgumentException("Invalid key: " + key);
      }
      var nv = v + d;
      if (Long.compareUnsigned(nv, v) < 0 || Long.compareUnsigned(nv, d) < 0) {
        throw new IllegalArgumentException("Invalid key: " + key);
      }
      v = nv;
      a *= 0x25;
    }
    return v;
  }

  public static String encode(long key) {
    var buf = new byte[14];
    int i = 14;
    do {
      buf[--i] = switch ((byte) Long.remainderUnsigned(key, 0x25)) {
        case 0x00 -> '_';
        case 0x01 -> '0';
        case 0x02 -> '1';
        case 0x03 -> '2';
        case 0x04 -> '3';
        case 0x05 -> '4';
        case 0x06 -> '5';
        case 0x07 -> '6';
        case 0x08 -> '7';
        case 0x09 -> '8';
        case 0x0A -> '9';
        case 0x0B -> 'a';
        case 0x0C -> 'b';
        case 0x0D -> 'c';
        case 0x0E -> 'd';
        case 0x0F -> 'e';
        case 0x10 -> 'f';
        case 0x11 -> 'g';
        case 0x12 -> 'h';
        case 0x13 -> 'i';
        case 0x14 -> 'j';
        case 0x15 -> 'k';
        case 0x16 -> 'l';
        case 0x17 -> 'm';
        case 0x18 -> 'n';
        case 0x19 -> 'o';
        case 0x1A -> 'p';
        case 0x1B -> 'q';
        case 0x1C -> 'r';
        case 0x1D -> 's';
        case 0x1E -> 't';
        case 0x1F -> 'u';
        case 0x20 -> 'v';
        case 0x21 -> 'w';
        case 0x22 -> 'x';
        case 0x23 -> 'y';
        case 0x24 -> 'z';
        default -> throw new IllegalArgumentException("Invalid key: " + key);
      };
      key = Long.divideUnsigned(key, 0x25);
    } while (Long.compareUnsigned(key, 0L) != 0);
    return new String(buf, i, 14 - i, ISO_8859_1);
  }
}
