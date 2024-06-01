package org.dauch.piola.benchmark.bytebuffer;

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

import lombok.AllArgsConstructor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Fork(value = 1, jvmArgs = "-Xmx4g")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 3)
@Warmup(iterations = 3)
public class ByteBufferFindBenchmark {

  private static final int OPS = 100;

  @Benchmark
  @OperationsPerInvocation(OPS)
  public void benchmark(ByteBufferState state, Blackhole bh) {
    var finder = state.finder;
    for (var b : state.toFind) {
      bh.consume(finder.find(b));
    }
  }

  @State(Scope.Benchmark)
  public static class ByteBufferState {

    @Param({"64", "128", "1024", "8192"})
    private int size;

    @Param({"array", "hash"})
    private String type;

    private BufferFinder finder;
    private ByteBuffer[] toFind;

    @Setup
    public void setup() {
      var buffers = new ByteBuffer[size];
      finder = switch (type) {
        case "array" -> new ArrayFinder(buffers);
        case "hash" -> new IdentityHashFinder(buffers);
        default -> throw new IllegalArgumentException(type);
      };
      var random = new Random(0L);
      toFind = IntStream.range(0, OPS)
        .mapToObj(_ -> buffers[random.nextInt(size)])
        .toArray(ByteBuffer[]::new);
    }
  }

  public static void main(String... args) throws Exception {
    var runner = new Runner(new OptionsBuilder()
      .include(MethodHandles.lookup().lookupClass().getName())
      .build());
    runner.run();
  }

  interface BufferFinder {
    int find(ByteBuffer buffer);
  }

  @AllArgsConstructor
  private static final class ArrayFinder implements BufferFinder {

    private final ByteBuffer[] array;

    @Override
    public int find(ByteBuffer buffer) {
      for (int i = 0, l = array.length; i < l; i++) {
        if (array[i] == buffer) {
          return i;
        }
      }
      return -1;
    }
  }

  public static final class IdentityHashFinder implements BufferFinder {

    private final IdentityHashMap<ByteBuffer, Integer> map;

    public IdentityHashFinder(ByteBuffer[] buffers) {
      this.map = new IdentityHashMap<>(buffers.length);
      for (int i = 0; i < buffers.length; i++) {
        map.put(buffers[i], i);
      }
    }

    @Override
    public int find(ByteBuffer buffer) {
      var i = map.get(buffer);
      return i == null ? -1 : i;
    }
  }
}
