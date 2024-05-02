package org.dauch.piola.benchmark.mapped;

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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.System.getProperty;
import static java.lang.System.nanoTime;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;
import static java.util.UUID.randomUUID;

@Fork(value = 1, jvmArgs = "-Xmx4g")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MappedArenaBenchmarks {

  private static final int OPS = 10;

  @Benchmark
  @Measurement(iterations = 3, batchSize = OPS)
  @Warmup(iterations = 3, batchSize = OPS)
  @OperationsPerInvocation(OPS)
  public void fill(FileState state) {
    state.segment.fill((byte) 0);
  }

  @State(Scope.Benchmark)
  public static class FileState {

    private static final int SIZE = 10 << 20;

    private final FileChannel channel;
    private final FileChannel randomChannel;
    private final MemorySegment segment;
    private final MemorySegment randomSegment;

    public FileState() {
      var opts = EnumSet.of(CREATE_NEW, READ, WRITE, SPARSE, DELETE_ON_CLOSE);
      try {
        channel = open(Path.of(getProperty("java.io.tmpdir"), randomUUID() + "-" + nanoTime() + ".data"), opts);
        randomChannel = open(Path.of(getProperty("java.io.tmpdir"), randomUUID() + "-" + nanoTime() + ".data"), opts);
        segment = channel.map(READ_WRITE, 0L, SIZE, Arena.ofAuto());
        randomSegment = randomChannel.map(READ_WRITE, 0L, SIZE, Arena.ofAuto());
        var random = new Random(0L);
        for (int i = 0; i < SIZE; i += 4) {
          randomSegment.set(JAVA_INT, i, random.nextInt());
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    @Setup(Level.Iteration)
    public void beforeEachIteration() {
      segment.copyFrom(randomSegment);
    }

    @TearDown(Level.Iteration)
    public void close() throws Exception {
      channel.close();
    }
  }

  public static void main(String... args) throws Exception {
    var runner = new Runner(new OptionsBuilder()
      .include(MethodHandles.lookup().lookupClass().getName())
      .threads(1)
      .build());
    runner.run();
  }
}
