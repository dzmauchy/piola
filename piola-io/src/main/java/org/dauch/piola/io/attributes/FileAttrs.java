package org.dauch.piola.io.attributes;

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

import org.dauch.piola.io.exception.NoValueException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.EnumSet;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

public final class FileAttrs extends Attrs implements AutoCloseable {

  private static final ValueLayout.OfLong LONG = JAVA_LONG.withOrder(ByteOrder.BIG_ENDIAN);

  private final Path path;
  private Arena arena;
  private MemorySegment segment;

  public FileAttrs(Path path, boolean readOnly) {
    this.path = path;
    this.arena = Arena.ofShared();
    try {
      var opts = readOnly ? EnumSet.of(READ) : EnumSet.of(READ, WRITE, CREATE);
      try (var ch = FileChannel.open(path, opts)) {
        segment = ch.map(readOnly ? READ_ONLY : READ_WRITE, 0L, ch.size(), arena);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int size() {
    return (int) (segment.byteSize() >>> 4);
  }

  @Override
  public long getKeyByIndex(int index) {
    return segment.getAtIndex(LONG, index * 2L);
  }

  @Override
  public long getValueByIndex(int index) {
    return segment.getAtIndex(LONG, index * 2L + 1L);
  }

  public void update(SimpleAttrs attrs) {
    var ks = attrs.keys;
    var vs = attrs.values;
    if (size() == 0) {
      arena.close();
      arena = Arena.ofShared();
      try (var ch = FileChannel.open(path, EnumSet.of(READ, WRITE, CREATE))) {
        segment = ch.map(READ_WRITE, 0L, (long) attrs.size() << 4, arena);
        for (int i = 0, l = attrs.keys.length; i < l; i++) {
          segment.setAtIndex(LONG, i * 2L, attrs.keys[i]);
          segment.setAtIndex(LONG, i * 2L + 1L, attrs.values[i]);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      int toAdd = 0;
      var size = size();
      for (var k : attrs.keys) {
        if (binarySearch(k, size) < 0) toAdd++;
      }
      arena.close();
      arena = Arena.ofShared();
      try (var ch = FileChannel.open(path, EnumSet.of(READ, WRITE, CREATE))) {
        segment = ch.map(READ_WRITE, 0L, ch.size() + ((long) toAdd << 4), arena);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      for (int i = 0, l = ks.length; i < l; i++) {
        put(size++, ks[i], vs[i]);
      }
    }
  }

  private void put(int size, long key, long value) {
    var i = binarySearch(key, size);
    if (i >= 0) {
      segment.setAtIndex(LONG, i * 2L + 1L, value);
    } else {
      i = -(i + 1);
      for (int j = size; j > i; j--) {
        segment.setAtIndex(LONG, j * 2L, segment.getAtIndex(LONG, (j - 1) * 2L));
        segment.setAtIndex(LONG, j * 2L + 1, segment.getAtIndex(LONG, (j - 1) * 2L + 1L));
      }
      segment.setAtIndex(LONG, i * 2L, key);
      segment.setAtIndex(LONG, i * 2L + 1L, value);
    }
  }

  @Override
  long readRaw(long key) throws NoValueException {
    int i = binarySearch(key, size());
    if (i >= 0) {
      return segment.getAtIndex(LONG, i * 2L);
    } else {
      throw NoValueException.NO_VALUE_EXCEPTION;
    }
  }

  private int binarySearch(long key, int size) {
    int l = 0, h = size - 1;
    while (l <= h) {
      int m = (l + h) >>> 1;
      long v = segment.getAtIndex(LONG, m * 2L);
      if (key < v) l = m + 1;
      else if (key > v) h = m - 1;
      else return m;
    }
    return -(l + 1);
  }

  @Override
  public void write(ByteBuffer buffer) {
    buffer.putInt(size()).put(segment.asByteBuffer());
  }

  public SimpleAttrs toSimpleAttrs() {
    var l = size();
    var ks = new long[l];
    var vs = new long[l];
    for (int i = 0; i < l; i++) {
      ks[i] = segment.getAtIndex(LONG, i * 2L);
      vs[i] = segment.getAtIndex(LONG, i * 2L + 1L);
    }
    return new SimpleAttrs(ks, vs);
  }

  @Override
  public void close() {
    arena.close();
  }
}
