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

import org.dauch.piola.buffer.UniqueIndexBuffer.Ch;
import org.dauch.piola.buffer.UniqueIndexBuffer.UCh;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.dauch.piola.buffer.UniqueIndexBuffer.channel;
import static org.dauch.piola.util.Hex.hexDir;

public final class IndexBuffer implements AutoCloseable {

  private final Path directory;
  private final ConcurrentSkipListMap<Path, FCh> fChannels = new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<Path, UCh> bChannels = new ConcurrentSkipListMap<>();

  public IndexBuffer(Path directory) {
    this.directory = directory;
  }

  public void add(long index, long offset) {
    var fch = channel(hexDir(index, directory.resolve("f")), fChannels, FCh::new, 64);
    fch.set(index, offset);
  }

  @Override
  public void close() {
    fChannels.values().removeIf(e -> {
      e.close();
      return true;
    });
    bChannels.values().removeIf(e -> {
      e.close();
      return true;
    });
  }

  private static final class FCh extends Ch {

    private final Path path;
    private final ConcurrentSkipListMap<Character, FileChannel> cache = new ConcurrentSkipListMap<>();

    private FCh(Path path) {
      this.path = path;
    }

    public void set(long k, long v) {
      var kc = (char) (k & 0xFFFFL);

    }

    @Override
    void close() {
    }
  }
}
