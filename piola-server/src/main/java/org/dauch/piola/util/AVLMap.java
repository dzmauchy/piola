package org.dauch.piola.util;

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

import org.dauch.piola.exception.DataCorruptionException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongConsumer;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.dauch.piola.buffer.BufferManager.unmapBuffers;

/**
 * AVLMap implementation based on FileChannel
 */
public final class AVLMap implements AutoCloseable {

  // common constants
  static final int FILE_HEADER_SIZE = 128;
  static final long VERSION = 1;

  // header offsets
  static final int H_VERSION = 0; // file version
  static final int H_ROOT = H_VERSION + Long.BYTES; // root node address
  static final int H_LAST = H_ROOT + Long.BYTES; // last position

  // node arithmetics
  static final int KEY = 0;
  static final int VALUE = KEY + Long.BYTES;
  static final int NEXT = VALUE + Long.BYTES;
  static final int LEFT = NEXT + Long.BYTES;
  static final int RIGHT = LEFT + Long.BYTES;
  static final int HEIGHT = RIGHT + Long.BYTES;
  static final int RESERVED = HEIGHT + Integer.BYTES;
  static final int NODE_SIZE = RESERVED + Integer.BYTES;

  // file channel
  final FileChannel channel;

  // header management objects
  private final Arena headerArena = Arena.ofShared();
  private final MemorySegment header;

  // segments cache
  private final int segmentSize;
  private final int maxSegments;
  private final ConcurrentSkipListMap<Long, MemorySegment> segments = new ConcurrentSkipListMap<>(Long::compareTo);

  private AVLMap(FileChannel channel, int segmentSize, int maxSegments) {
    this.channel = channel;
    this.segmentSize = segmentSize;
    this.maxSegments = maxSegments;
    try {
      header = channel.map(READ_WRITE, 0L, FILE_HEADER_SIZE, headerArena);
      if (header.get(JAVA_LONG, H_LAST) == 0L) {
        header.set(JAVA_LONG, H_VERSION, VERSION);
        header.set(JAVA_LONG, H_ROOT, -1L);
        header.set(JAVA_LONG, H_LAST, FILE_HEADER_SIZE);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public AVLMap(Path path, int segmentSize, int maxSegments) {
    this(channel(path), checkSegmentSize(segmentSize), checkMaxSegments(maxSegments));
  }

  public void get(long key, LongConsumer consumer) {
    var node = get(node(root()), key);
    if (node != null) {
      node.forEachValue(consumer);
    }
  }

  private VirtualNode get(VirtualNode node, long key) {
    if (node == null) {
      return null;
    } else if (key < node.getKey()) {
      return get(node.getLeft(), key);
    } else if (key > node.getKey()) {
      return get(node.getRight(), key);
    } else {
      return node;
    }
  }

  public synchronized void put(long key, long value) {
    var root = root();
    var node = put(node(root), key, value);
    if (root != node.node) {
      header.set(JAVA_LONG, H_ROOT, node.node);
    }
  }

  private VirtualNode put(VirtualNode node, long key, long value) {
    if (node == null) {
      return write(key, value);
    } else {
      if (key < node.getKey()) {
        node.setLeft(put(node.getLeft(), key, value));
      } else if (key > node.getKey()) {
        node.setRight(put(node.getRight(), key, value));
      } else {
        node.add(value);
        return node;
      }
      return balance(node);
    }
  }

  private int height(VirtualNode node) {
    return node == null ? 0 : node.getHeight();
  }

  private void updateHeight(VirtualNode node) {
    node.setHeight(1 + Math.max(height(node.getLeft()), height(node.getRight())));
  }

  private int balanceFactor(VirtualNode node) {
    return node == null ? 0 : height(node.getLeft()) - height(node.getRight());
  }

  private VirtualNode balance(VirtualNode node) {
    updateHeight(node);
    int bf = balanceFactor(node);
    if (bf > 1) {
      var left = node.getLeft();
      if (height(left.getLeft()) < height(left.getRight())) {
        node.setLeft(rotateLeft(left));
      }
      return rotateRight(node);
    } else if (bf < -1) {
      var right = node.getRight();
      if (height(right.getLeft()) > height(right.getRight())) {
        node.setRight(rotateRight(right));
      }
      return rotateLeft(node);
    }
    return node;
  }

  private VirtualNode rotateRight(VirtualNode n) {
    var rotated = n.getLeft();
    n.setLeft(rotated.getRight());
    rotated.setRight(n);
    updateHeight(n);
    updateHeight(rotated);
    return rotated;
  }

  private VirtualNode rotateLeft(VirtualNode n) {
    var rotated = n.getRight();
    n.setRight(rotated.getLeft());
    rotated.setLeft(n);
    updateHeight(n);
    updateHeight(rotated);
    return rotated;
  }

  private VirtualNode write(long key, long value) {
    var last = header.get(JAVA_LONG, H_LAST);
    header.set(JAVA_LONG, H_LAST, last + NODE_SIZE);
    var entry = segment(last);
    var base = (int) (last - entry.offset);
    var segment = entry.segment;
    segment.set(JAVA_LONG, base + KEY, key);
    segment.set(JAVA_LONG, base + VALUE, value);
    segment.set(JAVA_LONG, base + NEXT, -1L);
    segment.set(JAVA_LONG, base + LEFT, -1L);
    segment.set(JAVA_LONG, base + RIGHT, -1L);
    segment.set(JAVA_INT, base + HEIGHT, 1);
    segment.set(JAVA_INT, base + RESERVED, 0);
    return new VirtualNode(last, segment, base);
  }

  private SegmentEntry segment(long offset) {
    var entry = segments.ceilingEntry(offset - segmentSize + NODE_SIZE);
    if (entry == null || entry.getKey() > offset) {
      try {
        var memorySegment = channel.map(READ_WRITE, offset, segmentSize, Arena.ofAuto());
        var old = segments.put(offset, memorySegment);
        if (old != null) {
          old.force();
        }
        clean(memorySegment);
        return new SegmentEntry(offset, memorySegment);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      return new SegmentEntry(entry.getKey(), entry.getValue());
    }
  }

  private void clean(MemorySegment segment) {
    while (segments.size() > maxSegments) {
      for (var it = segments.values().iterator(); it.hasNext(); ) {
        var s = it.next();
        if (s != segment && segments.size() > maxSegments) {
          it.remove();
          s.force();
          if (segments.size() <= maxSegments) {
            return;
          }
        }
      }
    }
  }

  private static FileChannel channel(Path path) {
    try {
      return FileChannel.open(path, CREATE, WRITE, READ);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private VirtualNode node(long node) {
    return node < 0L ? null : new VirtualNode(node);
  }

  /**
   * It will be used in tests to flush changes to the file
   */
  void flush() {
    header.force();
    segments.forEach((_, v) -> v.force());
  }

  long root() {
    return header.get(JAVA_LONG, H_ROOT);
  }

  @Override
  public void close() {
    try (channel; headerArena) {
      var exception = new IllegalArgumentException();
      unmapBuffers(c -> segments.entrySet().removeIf(e -> {
        var segment = e.getValue();
        try {
          segment.force();
        } catch (Throwable x) {
          exception.addSuppressed(new DataCorruptionException("Cannot close a segment at " + e.getKey(), x));
        }
        c.accept(segment);
        return true;
      }));
      header.force();
      if (exception.getSuppressed().length > 0) {
        throw exception;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static int checkMaxSegments(int maxSegments) {
    if (maxSegments < 2) {
      throw new IllegalArgumentException("maxSegments must be at least 2");
    }
    return maxSegments;
  }

  private static int checkSegmentSize(int segmentSize) {
    if (segmentSize < 1024) {
      throw new IllegalArgumentException("segmentSize must be at least 1024");
    }
    if (segmentSize % Long.BYTES != 0) {
      throw new IllegalArgumentException("segmentSize should be aligned to 64 bit");
    }
    return segmentSize;
  }

  private boolean couldUseTheSameSegment(long addr, long offset, int dataSize) {
    return addr >= offset && addr <= offset + segmentSize - dataSize;
  }

  private final class VirtualNode {

    private final long node;
    private final MemorySegment segment;
    private final int base;

    private VirtualNode(long node) {
      this.node = node;
      var entry = segment(node);
      this.segment = entry.segment;
      this.base = (int) (node - entry.offset);
    }

    private VirtualNode(long node, MemorySegment segment, int base) {
      this.node = node;
      this.segment = segment;
      this.base = base;
    }

    private void add(long value) {
      var pValue = segment.get(JAVA_LONG, base + VALUE);
      var pNext = segment.get(JAVA_LONG, base + NEXT);
      var last = header.get(JAVA_LONG, H_LAST);
      header.set(JAVA_LONG, H_LAST, last + 16);
      var lastSegmentEntry = valueSegmentOf(last);
      var lastSegment = lastSegmentEntry.segment;
      var lastSegmentOffset = last - lastSegmentEntry.offset;
      lastSegment.set(JAVA_LONG, lastSegmentOffset, pValue);
      lastSegment.set(JAVA_LONG, lastSegmentOffset + 8, pNext);
      segment.set(JAVA_LONG, base + VALUE, value);
      segment.set(JAVA_LONG, base + NEXT, last);
    }

    private long getKey() {
      return segment.get(JAVA_LONG, base + KEY);
    }

    private VirtualNode getLeft() {
      var left = segment.get(JAVA_LONG, base + LEFT);
      return left < 0L ? null : nodeOf(left);
    }

    private VirtualNode getRight() {
      var right = segment.get(JAVA_LONG, base + RIGHT);
      return right < 0L ? null : nodeOf(right);
    }

    private VirtualNode nodeOf(long node) {
      var offset = this.node - base;
      if (couldUseTheSameSegment(node, offset, NODE_SIZE)) {
        return new VirtualNode(node, segment, (int) (node - offset));
      } else {
        return new VirtualNode(node);
      }
    }

    private SegmentEntry valueSegmentOf(long last) {
      var offset = node - base;
      if (couldUseTheSameSegment(last, offset, 16)) {
        return new SegmentEntry(offset, segment);
      } else {
        return segment(last);
      }
    }

    private void setLeft(VirtualNode node) {
      segment.set(JAVA_LONG, base + LEFT, node == null ? -1L : node.node);
    }

    private void setRight(VirtualNode node) {
      segment.set(JAVA_LONG, base + RIGHT, node == null ? -1L : node.node);
    }

    private int getHeight() {
      return segment.get(JAVA_INT, base + HEIGHT);
    }

    private void setHeight(int height) {
      segment.set(JAVA_INT, base + HEIGHT, height);
    }

    private void forEachValue(LongConsumer consumer) {
      consumer.accept(segment.get(JAVA_LONG, base + VALUE));
      for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0L; ) {
        if (couldUseTheSameSegment(n, node - base, 16)) {
          var o = n - (node - base);
          consumer.accept(segment.get(JAVA_LONG, o));
          n = segment.get(JAVA_LONG, o + 8);
        } else {
          var entry = segment(n);
          var s = entry.segment;
          var o = n - entry.offset;
          consumer.accept(s.get(JAVA_LONG, o));
          n = s.get(JAVA_LONG, o + 8);
        }
      }
    }
  }

  private record SegmentEntry(long offset, MemorySegment segment) {}
}
