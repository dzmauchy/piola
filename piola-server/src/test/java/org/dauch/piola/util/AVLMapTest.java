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

import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.*;

import static org.dauch.piola.util.AVLMap.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

  private static long[] get(AVLMap map, long key) {
    var streamBuilder = LongStream.builder();
    map.get(key, streamBuilder::add);
    return streamBuilder.build().toArray();
  }

  @ParameterizedTest
  @MethodSource("randomDataToPut")
  void putAndGet(long[] keys, long[] values) {
    var map = new TreeMap<Long, TreeSet<Long>>();
    try (var actualMap = new AVLMap(file, 1 << 20, 1024)) {
      for (int i = 0; i < keys.length; i++) {
        map.computeIfAbsent(keys[i], _ -> new TreeSet<>()).add(values[i]);
        actualMap.put(keys[i], values[i]);
      }
      for (var key : Arrays.stream(keys).boxed().collect(Collectors.toSet())) {
        var expected = map.get(key);
        var actual = new TreeSet<Long>();
        actualMap.get(key, actual::add);
        assertEquals(expected, actual);
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
      actualMap.flush();
      final class NodeComparator {

        private final ByteBuffer nodeBuffer = ByteBuffer.allocateDirect(NODE_SIZE).order(ByteOrder.nativeOrder());
        private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
        private final FileChannel channel = actualMap.channel;

        private void compare(AVLMemoryMap.Node node, long address, ImmutableLongSet stack) throws Exception {
          // check null node
          if (node == null) {
            assert address < 0 : "Address is not negative: " + address + ", stack = " + stack;
            return;
          }
          // read node
          channel.read(nodeBuffer.slice(), address);
          // check node fields
          var actualKey = nodeBuffer.getLong(KEY);
          var actualHeight = nodeBuffer.getLong(HEIGHT);
          assert actualKey == node.key : "expectedKey " + node.key + ", actualKey=" + actualKey + ", stack = " + stack;
          assert actualHeight == node.height : "expectedHeight " + node.height + ", actualHeight = " + actualHeight + ", stack = " + stack;
          // check values
          var expectedValuesBuilder = LongStream.builder();
          var actualValuesBuilder = LongStream.builder();
          for (var e = node.value; e != null; e = e.prev()) {
            expectedValuesBuilder.add(e.value());
          }
          actualValuesBuilder.add(nodeBuffer.getLong(VALUE));
          for (var a = nodeBuffer.getLong(NEXT); a >= 0L; ) {
            channel.read(valueBuffer.slice(), a);
            actualValuesBuilder.add(valueBuffer.getLong(0));
            a = valueBuffer.getLong(8);
          }
          assertArrayEquals(
            expectedValuesBuilder.build().toArray(),
            actualValuesBuilder.build().toArray(),
            () -> "Values mismatch, stack = " + stack
          );
          // get left and right
          var leftAddress = nodeBuffer.getLong(LEFT);
          var rightAddress = nodeBuffer.getLong(RIGHT);
          // check left
          compare(node.left, leftAddress, stack.newWith(node.key));
          // check right
          compare(node.right, rightAddress, stack.newWith(node.key));
        }
      }
      var comparator = new NodeComparator();
      comparator.compare(expectedMap.root, actualMap.root(), LongSets.immutable.empty());
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
}
