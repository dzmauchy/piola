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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

final class FastBitSetTest {

  @Test
  void empty() {
    // given
    var bs = new FastBitSet(1);
    // then
    assertTrue(bs.isEmpty());
    assertFalse(bs.nonEmpty());
    assertEquals(0, bs.cardinality());
    assertEquals("{}", bs.toString());
    assertEquals(-1, bs.nextSetBit(0));

    // given
    var buf = ByteBuffer.allocate(100);
    // when
    bs.write(buf);
    // then
    assertEquals(2 + Long.BYTES, buf.position());

    // when
    var newBs = new FastBitSet(buf.flip());
    // then
    assertEquals(bs, newBs);
  }

  @Test
  void nonEmpty() {
    // given
    var bs = new FastBitSet(129);
    // when
    bs.set(0);
    bs.set(63);
    bs.set(65);
    bs.set(127);
    bs.set(128);
    // then
    assertFalse(bs.isEmpty());
    assertTrue(bs.nonEmpty());
    assertEquals(5, bs.cardinality());
    assertEquals(0, bs.nextSetBit(0));
    assertEquals(63, bs.nextSetBit(1));
    assertEquals(63, bs.nextSetBit(63));
    assertEquals(65, bs.nextSetBit(64));
    assertEquals(65, bs.nextSetBit(65));
    assertEquals(127, bs.nextSetBit(66));
    assertEquals(127, bs.nextSetBit(127));
    assertEquals(128, bs.nextSetBit(128));
    assertEquals(-1, bs.nextSetBit(129));
    assertEquals("{0,63,65,127,128}", bs.toString());

    // given
    var buf = ByteBuffer.allocate(100);
    // when
    bs.write(buf);
    // then
    assertEquals(2 + 3 * Long.BYTES, buf.position());

    // when
    var newBs = new FastBitSet(buf.flip());
    // then
    assertEquals(bs, newBs);
  }

  @Test
  void clearAndSet() {
    // given
    var bs = new FastBitSet(8);
    bs.set(4);
    bs.set(3);
    bs.set(5);
    bs.set(2);
    // when
    bs.clear(3);
    bs.clear(5);
    // then
    assertTrue(bs.get(4));
    assertTrue(bs.get(2));
    assertFalse(bs.get(3));
    assertFalse(bs.get(5));
    assertEquals("{2,4}", bs.toString());
  }
}
