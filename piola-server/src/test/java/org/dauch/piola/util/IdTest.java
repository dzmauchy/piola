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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IdTest {

  @ParameterizedTest
  @MethodSource
  void parameterized(long v) {
    var encoded = Id.encode(v);
    var decoded = Id.decode(encoded);
    assertEquals(v, decoded);
  }

  static Stream<Arguments> parameterized() {
    var builder = LongStream.builder();
    for (var i = Long.MIN_VALUE; i <= Long.MIN_VALUE + 1_000L; i++) {
      builder.add(i);
    }
    for (var i = Long.MAX_VALUE - 1000L; i < Long.MAX_VALUE; i++) {
      builder.add(i);
    }
    var random = new Random(0L);
    for (var i = 0; i < 1_000; i++) {
      builder.add(random.nextLong());
    }
    return builder.build().mapToObj(Arguments::of);
  }

  @Test
  void zero() {
    assertEquals(0L, Id.decode("_"));
    assertEquals("_", Id.encode(0L));
  }

  @Test
  void one() {
    assertEquals(1L, Id.decode("0"));
    assertEquals("0", Id.encode(1L));
  }

  @Test
  void prefixed() {
    assertEquals(Id.decode("0"), Id.decode("_0"));
    assertEquals(Id.decode("rcv_buf_size"), Id.decode("__rcv_buf_size"));
  }

  @Test
  void npe() {
    assertThrows(NullPointerException.class, () -> Id.decode(null));
  }

  @Test
  void iaeOnEmpty() {
    assertThrows(IllegalArgumentException.class, () -> Id.decode(""));
  }

  @Test
  void iaeOnInvalidChar() {
    assertThrows(IllegalArgumentException.class, () -> Id.decode("abc.as"));
  }
}
