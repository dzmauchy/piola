package org.dauch.piola.base;

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

import org.junit.jupiter.api.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MappedSegmentsTest {

  private Path file;

  @BeforeEach
  void before() throws Exception {
    file = Files.createTempFile(Path.of(System.getProperty("user.home")), "pmft", ".data");
  }

  @AfterEach
  void after() throws Exception {
    if (file != null) {
      Files.deleteIfExists(file);
    }
  }

  private static long aligned(long v) {
    return v - v % 8;
  }

  @Test
  void overlappedRegions() throws Exception {
    var random = new Random(0L);
    var size = 1 << 24;
    var iterations = 1000;
    var segmentCount = 1024;
    try (var ch = FileChannel.open(file, EnumSet.of(READ, WRITE, CREATE)); var arena = Arena.ofShared()) {
      var segment = ch.map(READ_WRITE, 0, size, arena);
      var segments = new MemorySegment[segmentCount];
      var offsets = new long[segmentCount];
      var lengths = new long[segmentCount];
      for (int i = 0; i < segmentCount; i++) {
        var offset = random.nextLong(size);
        var len = random.nextLong(size - offset);
        segments[i] = ch.map(READ_WRITE, offset, len, arena);
        offsets[i] = offset;
        lengths[i] = len;
      }
      try (var pool = Executors.newFixedThreadPool(16, Thread.ofVirtual().factory())) {
        for (var i = 0; i < iterations; i++) {
          var offset = aligned(random.nextLong(size));
          var value = random.nextLong();
          segment.set(JAVA_LONG, offset, value);
          var latch = new CountDownLatch(segmentCount);
          var mismatches = new AtomicInteger();
          for (int s = 0; s < segmentCount; s++) {
            var segmentIndex = s;
            pool.execute(() -> {
              var off = offsets[segmentIndex];
              if (offset >= off && offset + 4L < off + lengths[segmentIndex]) {
                var seg = segments[segmentIndex];
                var v = seg.get(JAVA_LONG, offset - off);
                if (v != value) {
                  mismatches.getAndIncrement();
                }
              }
              latch.countDown();
            });
          }
          latch.await();
          assertEquals(0, mismatches.get());
        }
      }
    }
  }
}
