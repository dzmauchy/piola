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

import org.dauch.piola.util.ByteBufferIntMap;

import java.io.*;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Character.MAX_RADIX;
import static java.lang.System.nanoTime;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;

public final class BufferManager implements Closeable {

  private final ByteBufferIntMap bufferMap;
  private final ByteBuffer[] buffers;
  private final MemorySegment[] segments;
  private final int count;
  private final float freeSpaceRatio;
  private final FileChannel channel;
  private final BitSet state;

  public BufferManager(String prefix, Path directory, int count, int maxBufferSize, float freeSpaceRatio) {
    this.buffers = new ByteBuffer[count];
    this.segments = new MemorySegment[count];
    this.count = count;
    this.freeSpaceRatio = freeSpaceRatio;
    this.state = new BitSet(count);
    var rand = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
    var name = prefix + "-" + nanoTime() + "-" + Integer.toString(rand, MAX_RADIX) + ".data";
    var file = directory.resolve(name);
    try {
      channel = open(file, EnumSet.of(CREATE_NEW, READ, WRITE, SPARSE, DELETE_ON_CLOSE));
      for (int i = count - 1; i >= 0; i--) {
        var buffer = buffers[i] = channel.map(READ_WRITE, (long) i * (long) maxBufferSize, maxBufferSize);
        segments[i] = MemorySegment.ofBuffer(buffer);
      }
      this.bufferMap = new ByteBufferIntMap(buffers);
    } catch (Throwable e) {
      if (BufferManager.this.channel != null) {
        try {
          BufferManager.this.channel.close();
        } catch (Throwable x) {
          e.addSuppressed(x);
        }
      }
      if (e instanceof IOException ioe) {
        throw new UncheckedIOException(ioe);
      } else {
        throw new IllegalStateException(e);
      }
    }
  }

  public BufferManager(String prefix, BufferConfig conf) {
    this(prefix, conf.bufferDir(), conf.bufferCount(), conf.maxMessageSize(), conf.freeRatio());
  }

  public ByteBuffer get() {
    int slot;
    do {
      synchronized (this) {
        if ((slot = state.nextClearBit(0)) < count) {
          state.set(slot);
          break;
        }
      }
      Thread.onSpinWait();
    } while (true);
    return buffers[slot];
  }

  public synchronized void release(ByteBuffer buffer) {
    var index = bufferMap.get(buffer);
    state.clear(index);
    if (needsCleanup(buffer)) {
      segments[index].fill((byte) 0);
    }
    buffer.clear();
  }

  public boolean needsCleanup(ByteBuffer buffer) {
    var ratio = buffer.limit() / (float) buffer.capacity();
    return ratio > freeSpaceRatio;
  }

  @Override
  public void close() throws IOException {
    try (channel) {
      state.set(0, count);
    }
  }
}
