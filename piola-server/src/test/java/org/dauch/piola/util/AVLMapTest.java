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

import org.dauch.piola.util.AVLMemoryMap.Node;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.*;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;
import static org.dauch.piola.util.AVLMap.*;
import static org.junit.jupiter.api.Assertions.*;

class AVLMapTest {

  private Path file;

  @BeforeEach
  void beforeEach(@TempDir Path dir) {
    file = dir.resolve("file.data");
  }

  @Test
  void simple() {
    try (var map = new AVLMap(file, 1 << 10, 1024)) {
      map.put(10L, 20L);
      assertArrayEquals(new long[]{20L}, get(map, 10L));
    }
  }

  @Test
  void twoValues() {
    try (var map = new AVLMap(file, 1 << 10, 1024)) {
      map.put(5L, 20L);
      map.put(5L, 30L);
      assertArrayEquals(new long[]{30L, 20L}, get(map, 5L));
    }
  }

  @Test
  void sorted() {
    try (var map = new AVLMap(file, 1 << 10, 1024)) {
      map.put(1L, 3L, Long::compare);
      map.put(1L, 2L, Long::compare);
      map.put(1L, 1L, Long::compare);
      map.put(1L, 4L, Long::compare);
      var actual = map.get(1L).toArray();
      assertArrayEquals(new long[]{1L, 2L, 3L, 4L}, actual);
      assertEquals(4L, map.countValues(1L));
      assertTrue(map.contains(1L, 3L, Long::compare));
      assertTrue(map.contains(1L, 2L, Long::compare));
      assertTrue(map.contains(1L, 4L, Long::compare));
      assertTrue(map.contains(1L, 1L, Long::compare));
      assertFalse(map.contains(1L, 0L, Long::compare));
    }
  }

  @Test
  void sorted_check_duplicates() {
    try (var map = new AVLMap(file, 1 << 10, 1024)) {
      map.put(1L, 3L, Long::compare);
      map.put(1L, 2L, Long::compare);
      map.put(1L, 2L, Long::compare);
      map.put(1L, 4L, Long::compare);
      var actual = map.get(1L).toArray();
      assertArrayEquals(new long[]{2L, 3L, 4L}, actual);
      assertEquals(3L, map.countValues(1L));
    }
  }

  private static long[] get(AVLMap map, long key) {
    var streamBuilder = LongStream.builder();
    map.get(key, streamBuilder::add);
    return streamBuilder.build().toArray();
  }

  @ParameterizedTest
  @MethodSource("randomDataToPut")
  void putAndGet(long[] keys, long[] values) {
    var map = new TreeMap<Long, LongArrayList>();
    try (var actualMap = new AVLMap(file, 1 << 20, 1024)) {
      for (int i = 0; i < keys.length; i++) {
        map.computeIfAbsent(keys[i], _ -> new LongArrayList()).addAtIndex(0, values[i]);
        actualMap.put(keys[i], values[i]);
      }
      for (var it = LongSets.immutable.of(keys).longIterator(); it.hasNext(); ) {
        var key = it.next();
        var expected = map.get(key);
        var actual = new LongArrayList();
        actualMap.get(key, actual::add);
        assertEquals(expected, actual);
        assertArrayEquals(expected.toArray(), actualMap.get(key).toArray());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("randomDataToPut")
  void compareStructureAfterPut(long[] keys, long[] values) throws Exception {
    var expectedMap = new AVLMemoryMap();
    try (var actualMap = new AVLMap(file, 1 << 20, 1024)) {
      for (int i = 0; i < keys.length; i++) {
        expectedMap.put(keys[i], values[i]);
        actualMap.put(keys[i], values[i]);
      }
      try (var ch = FileChannel.open(file, EnumSet.of(READ)); var arena = Arena.ofConfined()) {
        var segment = ch.map(READ_ONLY, 0L, ch.size(), arena);
        compare(segment, expectedMap.root, segment.get(JAVA_LONG, H_ROOT), LongLists.immutable.empty());
      }
    }
  }

  static Stream<Arguments> randomDataToPut() {
    var random = new Random(0L);
    return IntStream.of(
        1,
        1 << 3,
        1 << 6,
        1 << 10,
        1 << 16,
        1 << 20
      )
      .mapToObj(n -> {
        var keys = LongStream.generate(() -> random.nextLong(1000L)).limit(n).toArray();
        var values = LongStream.generate(() -> random.nextLong(1000L)).limit(n).toArray();
        return Arguments.of(keys, values);
      });
  }

  private void compare(MemorySegment segment, Node node, long address, ImmutableLongList stack) {
    // check null node
    if (node == null) {
      assert address < 0 : "Address is not negative: " + address + ", stack = " + stack;
      return;
    }
    // check node fields
    var actualKey = segment.get(JAVA_LONG, address + KEY);
    var actualHeight = segment.get(JAVA_LONG, address + HEIGHT);
    assert actualKey == node.key : "expectedKey " + node.key + ", actualKey=" + actualKey + ", stack = " + stack;
    assert actualHeight == node.height : "expectedHeight " + node.height + ", actualHeight = " + actualHeight + ", stack = " + stack;
    // check values
    var expectedValuesBuilder = LongStream.builder();
    var actualValuesBuilder = LongStream.builder();
    for (var e = node.value; e != null; e = e.prev()) {
      expectedValuesBuilder.add(e.value());
    }
    actualValuesBuilder.add(segment.get(JAVA_LONG, address + VALUE));
    for (var a = segment.get(JAVA_LONG, address + NEXT); a >= 0L; ) {
      actualValuesBuilder.add(segment.get(JAVA_LONG, a));
      a = segment.get(JAVA_LONG, a + 8);
    }
    assertArrayEquals(
      expectedValuesBuilder.build().toArray(),
      actualValuesBuilder.build().toArray(),
      () -> "Values mismatch, stack = " + stack
    );
    // get left and right
    // check left
    compare(segment, node.left, segment.get(JAVA_LONG, address + LEFT), stack.newWith(node.key));
    // check right
    compare(segment, node.right, segment.get(JAVA_LONG, address + RIGHT), stack.newWith(node.key));
  }
}
