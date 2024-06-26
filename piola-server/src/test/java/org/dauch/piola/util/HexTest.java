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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class HexTest {

  @ParameterizedTest
  @MethodSource
  void hexDir(long v, Path dir, Path expected) {
    var actual = Hex.hexDir(v, dir);
    assertEquals(expected, actual);
  }

  static Stream<Arguments> hexDir() {
    var basePath = Path.of("abc");
    return Stream.of(
      arguments(
        ByteBuffer.allocate(8)
          .putChar((char) 0xABCD)
          .putChar((char) 0x0134)
          .putChar((char) 0x2390)
          .putChar((char) 0x1110)
          .getLong(0),
        basePath,
        basePath.resolve("ABCD").resolve("01")
      ),
      arguments(
        ByteBuffer.allocate(8)
          .putChar((char) 0x0000)
          .putChar((char) 0x0001)
          .putChar((char) 0xFFFF)
          .putChar((char) 0x0101)
          .getLong(0),
        basePath,
        basePath.resolve("0000").resolve("00")
      )
    );
  }
}
