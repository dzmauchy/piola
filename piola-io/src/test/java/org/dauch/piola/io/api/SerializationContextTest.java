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

import org.dauch.piola.io.api.SerializationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
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
      .mapToObj(_ -> new BitSet())
      .peek(set -> {
        for (int i = 0, l = random.nextInt(100); i < l; i++) {
          set.set(random.nextInt(1, 256));
        }
      })
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
    assertEquals(0L, ctx.children().count());
    assertTrue(ctx.isEmpty());
  }

  @ParameterizedTest
  @MethodSource
  void serde(ByteBuffer buf, SerializationContext context) {
    // given
    context.write(buf.clear());
    // when
    var actual = SerializationContext.read(buf.flip());
    // then
    assertEquals(context, actual);
  }

  static Stream<Arguments> serde() {
    var buf = ByteBuffer.allocateDirect(4096);
    var random = new Random(0L);
    var builder = Stream.<Arguments>builder();
    builder.accept(Arguments.of(buf, new SerializationContext()));
    for (int i = 0; i < 100; i++) {
      var ctx = new SerializationContext();
      for (int j = 0, l = random.nextInt(100); j < l; j++) {
        if (random.nextBoolean()) {
          ctx.addUnknownField(random.nextInt(1, 256));
        } else {
          var child = ctx.child(random.nextInt(1, 256));
          if (random.nextBoolean()) {
            for (int k = 0, m = random.nextInt(100); k < m; k++) {
              child.addUnknownField(random.nextInt(1, 256));
            }
          }
        }
      }
      builder.accept(Arguments.of(buf, ctx));
    }
    return builder.build();
  }
}
