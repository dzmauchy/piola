package org.dauch.piola.api;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.BitSet;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SerializationContextTest {

  @ParameterizedTest
  @MethodSource
  void unknownFields(BitSet data) {
    // given
    var ctx = new SerializationContext();
    // when
    data.stream().forEach(ctx::addUnknownField);
    // then
    var actual = ctx.unknownFields().toArray();
    var expected = data.stream().toArray();
    assertArrayEquals(expected, actual);
  }

  static Stream<Arguments> unknownFields() {
    var random = new Random(0L);
    return IntStream.range(0, 100)
      .mapToObj(_ -> new byte[32])
      .peek(random::nextBytes)
      .map(BitSet::valueOf)
      .map(Arguments::of);
  }

  @Test
  void childrenTheSameInstanceOnTheSameId() {
    // given
    var ctx = new SerializationContext();
    // when
    for (int i = 0; i < 10; i++) {
      // given
      var expected = ctx.child(i);
      // when
      var actual = ctx.child(i);
      // then
      assertSame(expected, actual);
    }
    // then
    assertFalse(ctx.isEmpty());
    assertEquals(10, ctx.childrenCount());
    ctx.normalize();
    assertTrue(ctx.isEmpty());
    assertEquals(0, ctx.childrenCount());
  }
}
