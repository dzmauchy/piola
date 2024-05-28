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

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public final class Id {

  private Id() {
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
