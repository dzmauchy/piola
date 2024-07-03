package org.dauch.piola.benchmark.map;

/*-
 * #%L
 * piola-benchmarks
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

import org.dauch.piola.collections.map.AVLMap;
import org.dauch.piola.collections.map.AVLMemoryMap;
import org.dauch.piola.util.*;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import static java.lang.System.getProperty;
import static java.nio.file.Files.createTempDirectory;

@Fork(value = 1, jvmArgs = "-Xmx4g")
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 10, batchSize = 1)
@Warmup(iterations = 10, batchSize = 1)
@Threads(1)
public class AVLMapBenchmark {

  private static final int COUNT = 1 << 20;

  @Benchmark
  @OperationsPerInvocation(COUNT)
  public AVLMap disk(AVLDiskState state) {
    var map = state.map;
    var ks = state.keys;
    var vs = state.values;
    for (int i = 0, l = state.keys.length; i < l; i++) {
      map.put(ks[i], vs[i]);
    }
    return map;
  }

  @Benchmark
  @OperationsPerInvocation(COUNT)
  public AVLMemoryMap memory(AVLMemoryState state) {
    var map = new AVLMemoryMap();
    var ks = state.keys;
    var vs = state.values;
    for (int i = 0, l = state.keys.length; i < l; i++) {
      map.put(ks[i], vs[i]);
    }
    return map;
  }

  @State(Scope.Benchmark)
  public static class AVLDiskState extends AVLMemoryState {

    private final Path tempDir;
    private final AVLMap map;

    public AVLDiskState() {
      try {
        // we use user.home to avoid using /tmp directory which is mounted usually as a memory device
        tempDir = createTempDirectory(Path.of(getProperty("user.home")), "avl");
        map = new AVLMap(tempDir.resolve("data.data"), 1 << 20, 1024);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @TearDown
    public void close() throws IOException {
      try (var _ = (Closeable) () -> MoreFiles.deleteRecursively(tempDir)) {
        map.close();
      }
    }
  }

  @State(Scope.Benchmark)
  public static class AVLMemoryState {

    final long[] keys;
    final long[] values;

    public AVLMemoryState() {
      var random = new Random(0L);
      keys = LongStream.generate(() -> random.nextLong(1000L)).limit(COUNT).toArray();
      values = LongStream.generate(() -> random.nextLong(1000L)).limit(COUNT).toArray();
    }
  }

  public static void main(String... args) throws Exception {
    new Runner(new OptionsBuilder()
      .include(MethodHandles.lookup().lookupClass().getName())
      .build()).run();
  }
}
