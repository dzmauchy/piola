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

import java.io.File;
import java.nio.file.Path;

public interface Hex {

  private static void charToHex(char v, char[] chars, int offset) {
    for (int i = 3; i >= 0; i--, v >>>= 4) {
      chars[offset + i] = hex(v % 16);
    }
  }

  private static void byteToHex(byte v, char[] chars, int offset) {
    chars[offset] = hex((v >>> 4) & 0xF);
    chars[offset + 1] = hex(v & 0xF);
  }

  static char hex(int v) {
    return switch (v) {
      case 0x0 -> '0';
      case 0x1 -> '1';
      case 0x2 -> '2';
      case 0x3 -> '3';
      case 0x4 -> '4';
      case 0x5 -> '5';
      case 0x6 -> '6';
      case 0x7 -> '7';
      case 0x8 -> '8';
      case 0x9 -> '9';
      case 0xA -> 'A';
      case 0xB -> 'B';
      case 0xC -> 'C';
      case 0xD -> 'D';
      case 0xE -> 'E';
      case 0xF -> 'F';
      default -> throw new IllegalArgumentException(Integer.toString(v));
    };
  }

  static Path hexDir(long v, Path dir) {
    var name = new char[7];
    name[4] = File.separatorChar;
    charToHex((char) (v >>> 48), name, 0);
    byteToHex((byte) (v >>> 40), name, 5);
    return dir.resolve(new String(name));
  }
}
