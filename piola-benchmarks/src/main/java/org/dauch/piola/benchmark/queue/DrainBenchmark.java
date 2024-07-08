package org.dauch.piola.benchmark.queue;

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

import org.dauch.piola.util.DrainQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgs = "-Xmx4g")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Measurement(iterations = 3)
@Warmup(iterations = 3)
public class DrainBenchmark {

  private static final int QUEUE_SIZE = 128;
  private static final int PRODUCERS = 16;
  private static final Integer ELEMENT = 0;

  @Benchmark
  @Threads(PRODUCERS)
  public void ablWithArrayList(ABLWithArrayList state) throws Exception {
    state.queue.put(ELEMENT);
  }

  @Benchmark
  @Threads(PRODUCERS)
  public void drainQueueWithArray(DrainQueueWithArray state) throws Exception {
    state.queue.put(ELEMENT);
  }

  @State(Scope.Benchmark)
  public static class ABLWithArrayList {

    private final ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    private final Thread consumerThread = new Thread(this::consume);

    @Setup
    public void setup() {
      consumerThread.start();
    }

    @TearDown
    public void close() throws Exception {
      consumerThread.interrupt();
      consumerThread.join();
    }

    private void consume() {
      var thread = Thread.currentThread();
      var list = new ArrayList<Integer>(QUEUE_SIZE);
      while (!thread.isInterrupted()) {
        var count = queue.drainTo(list, Integer.MAX_VALUE);
        if (count == 0) {
          Thread.onSpinWait();
        } else {
          list.clear();
        }
      }
    }
  }

  @State(Scope.Benchmark)
  public static class DrainQueueWithArray {

    private final DrainQueue<Integer> queue = new DrainQueue<>(QUEUE_SIZE, Integer[]::new);
    private final Thread consumerThread = new Thread(this::consume);
    private final Integer[] list = new Integer[QUEUE_SIZE];

    @Setup
    public void setup() {
      consumerThread.start();
    }

    @TearDown
    public void close() throws Exception {
      consumerThread.interrupt();
      consumerThread.join();
    }

    private void consume() {
      var thread = Thread.currentThread();
      while (!thread.isInterrupted()) {
        var count = queue.drain(list);
        if (count == 0) {
          Thread.onSpinWait();
        }
      }
    }
  }

  public static void main(String... args) throws Exception {
    new Runner(new OptionsBuilder()
      .include(MethodHandles.lookup().lookupClass().getName())
      .build()).run();
  }
}
