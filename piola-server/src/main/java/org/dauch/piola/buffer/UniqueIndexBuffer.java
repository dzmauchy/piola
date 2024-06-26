package org.dauch.piola.buffer;

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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.*;
import static org.dauch.piola.buffer.BufferManager.CLEANER;
import static org.dauch.piola.util.Hex.hexDir;

public final class UniqueIndexBuffer implements AutoCloseable {

  private final Path directory;
  private final ConcurrentSkipListMap<Path, UCh> fChannels = new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<Path, UCh> bChannels = new ConcurrentSkipListMap<>();

  public UniqueIndexBuffer(Path directory) {
    this.directory = directory;
  }

  public void add(long index, long offset) {
    var fch = channel(hexDir(index, directory.resolve("f")), fChannels, UCh::new, 64);
    fch.set(index, offset);
    var bch = channel(hexDir(offset, directory.resolve("b")), bChannels, UCh::new, 4);
    bch.set(offset, index);
  }

  static <C extends Ch> C channel(Path path, ConcurrentSkipListMap<Path, C> channels, Function<Path, C> f, int limit) {
    var ch = channels.computeIfAbsent(path, f);
    ch.time = System.nanoTime();
    if (channels.size() > limit) {
      Thread.startVirtualThread(() -> channels.entrySet().stream()
        .filter(e -> e.getValue() != ch)
        .sorted(Comparator.comparingLong(e -> e.getValue().time))
        .limit(Math.max(0, channels.size() - limit))
        .forEach(e -> {
          var v = channels.remove(e.getKey());
          if (v != null) {
            v.close();
          }
        }));
    }
    return ch;
  }

  @Override
  public void close() {
    fChannels.values().removeIf(v -> {
      v.close();
      return true;
    });
    bChannels.values().removeIf(v -> {
      v.close();
      return true;
    });
  }

  static abstract class Ch {

    long time = System.nanoTime();

    abstract void close();
  }

  static final class UCh extends Ch {

    final FileChannel channel;
    final Cleaner.Cleanable cleanable;

    UCh(Path file) {
      try {
        var ch = channel = FileChannel.open(file, opts(file));
        cleanable = CLEANER.register(this, () -> {
          try {
            ch.close();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    void set(long k, long v) {
    }

    static EnumSet<StandardOpenOption> opts(Path path) {
      return Files.exists(path) ? EnumSet.of(READ, WRITE) : EnumSet.of(READ, WRITE, SPARSE, CREATE_NEW);
    }

    @Override
    void close() {
      cleanable.clean();
    }
  }
}
