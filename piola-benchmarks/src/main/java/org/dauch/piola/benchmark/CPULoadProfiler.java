package org.dauch.piola.benchmark;

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

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.*;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

public class CPULoadProfiler implements InternalProfiler {

  private final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
  private final DoubleSummaryStatistics statistics = new DoubleSummaryStatistics();

  private volatile double cpuLoad;

  {
    Thread.startVirtualThread(this::run);
  }

  @Override
  public String getDescription() {
    return "CPU Load Profiler";
  }

  @Override
  public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
  }

  @Override
  public Collection<? extends Result<?>> afterIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
    return List.of(new ScalarResult("cpu", cpuLoad, "%", AggregationPolicy.AVG));
  }

  private void run() {
    var thread = Thread.currentThread();
    while (!thread.isInterrupted()) {
      var usage = os.getSystemLoadAverage();
      if (usage >= 0d) {
        statistics.accept(usage);
        cpuLoad = statistics.getAverage() * 100d;
      }
      LockSupport.parkNanos(10_000_000L);
    }
  }
}
