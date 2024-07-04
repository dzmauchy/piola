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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AttributesTest {

  @Test
  void fileAttrs(@TempDir Path temp) {
    var attrsFile = temp.resolve("attrs.data");
    var simpleAttrs = new SimpleAttrs();
    simpleAttrs.putInt("abc", 1);
    simpleAttrs.putLong("cde", 2L);
    try (var attrs = new FileAttrs(attrsFile, false)) {
      attrs.update(simpleAttrs);
      var actual = attrs.toSimpleAttrs();
      assertEquals(simpleAttrs, actual);
    }
  }

  @Test
  void theSameBuffer(@TempDir Path temp) {
    var attrsFile = temp.resolve("attrs.data");
    var simpleAttrs = new SimpleAttrs();
    simpleAttrs.putInt("abc", 1);
    simpleAttrs.putLong("cde", 2L);
    try (var attrs = new FileAttrs(attrsFile, false)) {
      attrs.update(simpleAttrs);
      var expected = ByteBuffer.allocate(100);
      var actual = ByteBuffer.allocate(100);
      simpleAttrs.write(expected);
      attrs.write(actual);
      expected.flip();
      actual.flip();
      assertEquals(expected, actual, () -> {
        var expectedBytes = new byte[expected.remaining()];
        var actualBytes = new byte[actual.remaining()];
        expected.get(0, expectedBytes);
        actual.get(0, actualBytes);
        return HexFormat.of().formatHex(expectedBytes) + " vs " + HexFormat.of().formatHex(actualBytes);
      });
    }
  }
}
