package org.dauch.piola.benchmark.key;

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

import org.dauch.piola.util.Id;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import static java.lang.Long.MAX_VALUE;
import static java.util.stream.LongStream.range;

@Fork(value = 1, jvmArgs = "-Xmx4g")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Measurement(iterations = 3)
@Warmup(iterations = 3)
public class IdBenchmark {

  private static final int OPS = 100_000;

  @Benchmark
  @OperationsPerInvocation(OPS)
  public void decode(DecodeState state, Blackhole bh) {
    for (var s : state.data) {
      bh.consume(Id.decode(s));
    }
  }

  @Benchmark
  @OperationsPerInvocation(OPS)
  public void encode(Blackhole bh) {
    for (var v = MAX_VALUE - OPS; v < MAX_VALUE; v++) {
      bh.consume(Id.encode(v));
    }
  }

  @State(Scope.Benchmark)
  public static class DecodeState {
    private final String[] data = range(MAX_VALUE - OPS, MAX_VALUE).mapToObj(Id::encode).toArray(String[]::new);
  }

  public static void main(String... args) throws Exception {
    var runner = new Runner(new OptionsBuilder()
      .include(MethodHandles.lookup().lookupClass().getName())
      .build());
    runner.run();
  }
}
