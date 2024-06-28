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

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import static java.lang.Math.signum;
import static java.lang.System.Logger.Level.*;
import static java.lang.System.nanoTime;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;

public final class BufferManager implements Closeable {

  private static final VarHandle BUFFERS = MethodHandles.arrayElementVarHandle(ByteBuffer[].class);

  private final String prefix;
  private final ByteBuffer[] buffers;
  private final ByteBuffer[] buffersInUse;
  private final float freeSpaceRatio;
  private final FileChannel channel;
  private final Path file;

  public BufferManager(String prefix, Path directory, int count, int maxBufferSize, float freeSpaceRatio, boolean sparse) {
    this.prefix = prefix;
    this.buffers = new ByteBuffer[count];
    this.freeSpaceRatio = freeSpaceRatio;
    var name = prefix + Long.toUnsignedString(nanoTime(), 32) + ".data";
    this.file = directory.resolve(name);
    try {
      var opts = EnumSet.of(CREATE_NEW, READ, WRITE);
      if (sparse) {
        opts.add(SPARSE);
      }
      channel = open(file, opts);
      for (int i = count - 1; i >= 0; i--) {
        buffers[i] = channel.map(READ_WRITE, (long) i * (long) maxBufferSize, maxBufferSize);
      }
      buffersInUse = buffers.clone();
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
    this(prefix, conf.bufferDir(), conf.bufferCount(), conf.maxMessageSize(), conf.freeRatio(), conf.sparse());
  }

  private ByteBuffer get0() {
    for (int i = 0, l = buffers.length; i < l; i++) {
      var buf = buffers[i];
      if (BUFFERS.compareAndSet(buffersInUse, i, buf, null)) {
        return buf;
      }
    }
    return null;
  }

  public ByteBuffer get() {
    for (var buf = get0(); ; buf = get0()) {
      if (buf == null) {
        try {
          synchronized (buffers) {
            buffers.wait();
          }
        } catch (InterruptedException _) {
        }
      } else {
        return buf;
      }
    }
  }

  public void release(ByteBuffer buf) {
    var index = find(buf);
    if (needsCleanup(buf)) {
      cleanup(buf);
    }
    if (BUFFERS.compareAndSet(buffersInUse, index, null, buf.clear())) {
      synchronized (buffers) {
        buffers.notify();
      }
    } else {
      throw new IllegalStateException("Unable to return the buffer " + buf + " to the pool " + prefix);
    }
  }

  private void cleanup(ByteBuffer buffer) {
    for (int i = 0, c = buffer.capacity(); i < c; i++) {
      buffer.put(i, (byte) 0);
    }
  }

  public int find(ByteBuffer buffer) {
    var bs = buffers;
    for (int i = 0, l = bs.length; i < l; i++) {
      if (bs[i] == buffer) return i;
    }
    throw new IllegalStateException("Buffer " + buffer + " is unknown for " + prefix);
  }

  public boolean needsCleanup(ByteBuffer buffer) {
    var ratio = buffer.limit() / (float) buffer.capacity();
    return ratio > freeSpaceRatio;
  }

  public int maxBufferSize() {
    return buffers[0].capacity();
  }

  @Override
  public String toString() {
    return prefix;
  }

  public static void unmapBuffers(Consumer<Consumer<Object>> consumer) {
    var osName = System.getProperty("os.name");
    if (osName.toLowerCase().contains("windows")) {
      var queue = new ConcurrentLinkedQueue<WeakReference<?>>();
      consumer.accept(b -> queue.offer(new WeakReference<>(b)));
      var refs = new LinkedList<SoftReference<byte[]>>();
      for (long time = nanoTime(); nanoTime() - time < 10_000_000_000L; time += (int) signum(refs.hashCode())) {
        queue.removeIf(e -> e.get() == null);
        if (queue.isEmpty()) {
          return;
        }
        queue.removeIf(e -> {
          var v = e.get();
          if (v == null) {
            return true;
          } else {
            refs.add(new SoftReference<>(new byte[16384]));
            return false;
          }
        });
        System.gc();
      }
    } else {
      consumer.accept(_ -> {});
    }
  }

  @Override
  public void close() throws IOException {
    var logger = System.getLogger(getClass().getName());
    var set = new BitSet();
    for (int i = 0; i < buffers.length; i++) {
      if (BUFFERS.getAcquire(buffersInUse, i) == null) {
        set.set(i);
      }
    }
    if (!set.isEmpty()) {
      logger.log(ERROR, () -> prefix + " invalid state " + set);
    }
    try (channel) {
      unmapBuffers(c -> {
        for (int i = 0; i < buffers.length; i++) {
          var b = buffers[i];
          c.accept(b);
          BUFFERS.setRelease(buffers, i, null);
          BUFFERS.setRelease(buffersInUse, i, null);
        }
      });
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to close " + file, e);
    } finally {
      try {
        if (Files.exists(file)) {
          if (!Files.deleteIfExists(file)) {
            logger.log(WARNING, () -> "Unable to delete " + file);
          }
        }
      } catch (AccessDeniedException e) {
        logger.log(ERROR, e::getMessage);
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unexpected error on deleting file " + file, e);
      }
    }
  }
}
