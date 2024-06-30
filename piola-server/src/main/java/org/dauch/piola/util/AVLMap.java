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

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongConsumer;

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
  static final int NODE_SIZE = HEIGHT + Long.BYTES;

  // file channel
  private final FileChannel channel;

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
    } else {
      var nodeKey = node.getKey();
      if (key < nodeKey) {
        return get(node.getLeft(), key);
      } else if (key > nodeKey) {
        return get(node.getRight(), key);
      } else {
        return node;
      }
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
      var nodeKey = node.getKey();
      if (key < nodeKey) {
        node.setLeft(put(node.getLeft(), key, value));
      } else if (key > nodeKey) {
        node.setRight(put(node.getRight(), key, value));
      } else {
        node.add(value);
        return node;
      }
      return balance(node);
    }
  }

  private long height(VirtualNode node) {
    return node == null ? 0 : node.getHeight();
  }

  private void updateHeight(VirtualNode node) {
    node.setHeight(1L + Math.max(height(node.getLeft()), height(node.getRight())));
  }

  private int balanceFactor(VirtualNode node) {
    return node == null ? 0 : (int) (height(node.getLeft()) - height(node.getRight()));
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
    var last = getAndAdd(header, H_LAST, NODE_SIZE);
    var entry = segment(last);
    var base = (int) (last - entry.offset);
    var segment = entry.segment;
    segment.set(JAVA_LONG, base + KEY, key);
    segment.set(JAVA_LONG, base + VALUE, value);
    segment.set(JAVA_LONG, base + NEXT, -1L);
    segment.set(JAVA_LONG, base + LEFT, -1L);
    segment.set(JAVA_LONG, base + RIGHT, -1L);
    segment.set(JAVA_LONG, base + HEIGHT, 1L);
    return new VirtualNode(last, segment, base, segmentSize, this);
  }

  private SegmentEntry segment(long offset) {
    var entry = segments.ceilingEntry(offset - segmentSize + NODE_SIZE);
    if (entry == null || entry.getKey() > offset) {
      try {
        var memorySegment = channel.map(READ_WRITE, offset, segmentSize, Arena.ofAuto());
        var old = segments.put(offset, memorySegment);
        if (old != null) { // this shouldn't happen but if it happens the segment should be flushed on disk
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
    return node < 0L ? null : newNode(node);
  }

  private VirtualNode newNode(long node) {
    var entry = segment(node);
    return new VirtualNode(node, entry.segment, (int) (node - entry.offset), segmentSize, this);
  }

  private long root() {
    return header.get(JAVA_LONG, H_ROOT);
  }

  private static long getAndAdd(MemorySegment segment, long offset, long value) {
    return (long) JAVA_LONG.varHandle().getAndAdd(segment, offset, value);
  }

  @Override
  public void close() {
    try (channel; headerArena; var _ = (Closeable) header::force) {
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

  private static boolean couldUseTheSameSegment(long addr, long offset, int dataSize, int segmentSize) {
    return addr >= offset && addr <= offset + segmentSize - dataSize;
  }

  private record VirtualNode(long node, MemorySegment segment, int base, int segmentSize, AVLMap map) {

    private void add(long value) {
      var pValue = segment.get(JAVA_LONG, base + VALUE);
      var pNext = segment.get(JAVA_LONG, base + NEXT);
      var last = getAndAdd(map.header, H_LAST, 16L);
      var lastSegmentEntry = valueSegmentOf(last);
      var lastSegment = lastSegmentEntry.segment;
      var o = last - lastSegmentEntry.offset;
      lastSegment.set(JAVA_LONG, o, pValue);
      lastSegment.set(JAVA_LONG, o + 8, pNext);
      segment.set(JAVA_LONG, base + VALUE, value);
      segment.set(JAVA_LONG, base + NEXT, last);
    }

    private long getKey() {
      return segment.get(JAVA_LONG, base + KEY);
    }

    private VirtualNode getLeft() {
      return nodeOf(segment.get(JAVA_LONG, base + LEFT));
    }

    private VirtualNode getRight() {
      return nodeOf(segment.get(JAVA_LONG, base + RIGHT));
    }

    private VirtualNode nodeOf(long node) {
      if (node < 0L) {
        return null;
      }
      var offset = this.node - base;
      if (couldUseTheSameSegment(node, offset, NODE_SIZE, segmentSize)) {
        return new VirtualNode(node, segment, (int) (node - offset), segmentSize, map);
      } else {
        return map.newNode(node);
      }
    }

    private SegmentEntry valueSegmentOf(long last) {
      var offset = node - base;
      if (couldUseTheSameSegment(last, offset, 16, segmentSize)) {
        return new SegmentEntry(offset, segment);
      } else {
        return map.segment(last);
      }
    }

    private void setLeft(VirtualNode node) {
      segment.set(JAVA_LONG, base + LEFT, node == null ? -1L : node.node);
    }

    private void setRight(VirtualNode node) {
      segment.set(JAVA_LONG, base + RIGHT, node == null ? -1L : node.node);
    }

    private long getHeight() {
      return segment.get(JAVA_LONG, base + HEIGHT);
    }

    private void setHeight(long height) {
      segment.set(JAVA_LONG, base + HEIGHT, height);
    }

    private void forEachValue(LongConsumer consumer) {
      consumer.accept(segment.get(JAVA_LONG, base + VALUE));
      for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0L; ) {
        if (couldUseTheSameSegment(n, node - base, 16, segmentSize)) {
          var o = n - (node - base);
          consumer.accept(segment.get(JAVA_LONG, o));
          n = segment.get(JAVA_LONG, o + 8);
        } else {
          var entry = map.segment(n);
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
