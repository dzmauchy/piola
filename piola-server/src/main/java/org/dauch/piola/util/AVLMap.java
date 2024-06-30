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
import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.dauch.piola.buffer.BufferManager.unmapBuffers;

/**
 * AVLMap implementation based on Java 22 memory segments mapped to a file.
 * The implementation is not thread safe, the AVL tree restructuring or/and value insertions
 * could be partially visible for a reader thread. If one wants to use the map
 * from multiple threads use a {@link java.util.concurrent.locks.ReadWriteLock}
 * to synchronize the {@link AVLMap#get(long, LongConsumer)} method with a {@code readLock}
 * and the {@link AVLMap#put(long, long)} method with a {@code writeLock}.
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

  /**
   * Constructs an AVL disk based map.
   *
   * @param file        A file to use as storage
   * @param segmentSize Segment size (memory mapped segments of such size will be used as
   * @param maxSegments Maximum number of segments (segment evictions from the cache start from smaller offsets)
   */
  public AVLMap(Path file, int segmentSize, int maxSegments) {
    this.segmentSize = checkSegmentSize(segmentSize);
    this.maxSegments = checkMaxSegments(maxSegments);
    try {
      channel = FileChannel.open(file, EnumSet.of(CREATE, WRITE, READ));
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

  /**
   * Get values associated to a key.
   *
   * @param key      A given key
   * @param consumer Value consumer
   */
  public void get(long key, LongConsumer consumer) {
    var node = get(node(root()), key);
    if (node != null) {
      node.forEachValue(consumer);
    }
  }

  /**
   * Returns a lazy stream of values.
   *
   * @param key A given key
   * @return Stream of values
   */
  public LongStream get(long key) {
    var node = get(node(root()), key);
    return node == null ? LongStream.empty() : node.values();
  }

  /**
   * Returns true when the specified key is present.
   *
   * @param key Key
   * @return True if the key is present
   */
  public boolean contains(long key) {
    return get(node(root()), key) != null;
  }

  /**
   * Returns true when the specified key and value exist
   *
   * @param key   Key
   * @param value Value
   * @return Check status
   */
  public boolean contains(long key, long value) {
    var node = get(node(root()), key);
    return node != null && node.contains(value);
  }

  /**
   * Returns true if the specified key and value exist. The values should be
   * inserted using the same comparator.
   *
   * @param key Key
   * @param value Value
   * @param comparator Comparator
   * @return Check status
   */
  public boolean contains(long key, long value, LongBinaryOperator comparator) {
    var node = get(node(root()), key);
    return node != null && node.contains(value, comparator);
  }

  /**
   * Returns number of values associated with the given key
   *
   * @param key Key
   * @return Values count
   */
  public long countValues(long key) {
    var node = get(node(root()), key);
    return node == null ? 0L : node.countValues();
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

  /**
   * Inserts an entry to the map. The value will be inserted as a first value.
   *
   * @param key   Entry key
   * @param value Entry value
   */
  public void put(long key, long value) {
    var root = root();
    var node = put(node(root), key, value);
    if (root != node.node) {
      header.set(JAVA_LONG, H_ROOT, node.node);
    }
  }

  /**
   * Inserts an entry to the map. The value will be inserted according to
   * comparator. If {@code set} is true, duplications will be omitted. The caller code
   * is responsible for not using different comparators for the same key.
   *
   * @param key        Entry key
   * @param value      Entry value
   * @param comparator Value comparator
   */
  public void put(long key, long value, LongBinaryOperator comparator) {
    var root = root();
    var node = put(node(root), key, value, comparator);
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

  private VirtualNode put(VirtualNode node, long key, long value, LongBinaryOperator comparator) {
    if (node == null) {
      return write(key, value);
    } else {
      var nodeKey = node.getKey();
      if (key < nodeKey) {
        node.setLeft(put(node.getLeft(), key, value, comparator));
      } else if (key > nodeKey) {
        node.setRight(put(node.getRight(), key, value, comparator));
      } else {
        node.add(value, comparator);
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
    return (int) (height(node.getLeft()) - height(node.getRight()));
  }

  private VirtualNode balance(VirtualNode node) {
    var bf = balanceFactor(node);
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
    } else {
      updateHeight(node);
      return node;
    }
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
    var entry = segment(last, NODE_SIZE);
    var base = (int) (last - entry.offset);
    var segment = entry.segment;
    segment.set(JAVA_LONG, base + KEY, key);
    segment.set(JAVA_LONG, base + VALUE, value);
    segment.set(JAVA_LONG, base + NEXT, -1L);
    segment.set(JAVA_LONG, base + LEFT, -1L);
    segment.set(JAVA_LONG, base + RIGHT, -1L);
    segment.set(JAVA_LONG, base + HEIGHT, 1L);
    return new VirtualNode(segment, last, base, segmentSize, this);
  }

  private SegmentEntry segment(long offset, int size) {
    var entry = segments.ceilingEntry(offset - segmentSize + size);
    if (entry == null || entry.getKey() > offset) {
      try {
        var segment = channel.map(READ_WRITE, offset, segmentSize, Arena.ofAuto());
        segments.put(offset, segment);
        while (segments.size() > maxSegments) {
          for (var it = segments.values().iterator(); it.hasNext(); ) {
            var s = it.next();
            if (s != segment && segments.size() > maxSegments) {
              it.remove();
              if (segments.size() <= maxSegments) {
                return new SegmentEntry(offset, segment);
              }
            }
          }
        }
        return new SegmentEntry(offset, segment);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      return new SegmentEntry(entry.getKey(), entry.getValue());
    }
  }

  private VirtualNode node(long node) {
    return node < 0L ? null : newNode(node);
  }

  private VirtualNode newNode(long node) {
    var entry = segment(node, NODE_SIZE);
    return new VirtualNode(entry.segment, node, (int) (node - entry.offset), segmentSize, this);
  }

  private long root() {
    return header.get(JAVA_LONG, H_ROOT);
  }

  private static long getAndAdd(MemorySegment segment, long offset, long value) {
    return (long) JAVA_LONG.varHandle().getAndAdd(segment, offset, value);
  }

  private static long getAndSet(MemorySegment segment, long offset, long value) {
    return (long) JAVA_LONG.varHandle().getAndSet(segment, offset, value);
  }

  /**
   * Forces the cached segments to flush back to the storage, closes the file channel.
   * On Windows there is an issue of not being able to delete a file if there is at least
   * one buffer that wasn't unmapped. To overcome this issue we are using a trick to
   * wrap such buffers into {@link java.lang.ref.WeakReference} and then generate
   * a lot of garbage (typically it is a bunch of {@code byte[]} 65536-byte arrays)
   * until all weak references become enqueued that guarantees that associated cleaner actions
   * were already done.
   */
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

  private record VirtualNode(MemorySegment segment, long node, int base, int segmentSize, AVLMap map) {

    private void add(long value) {
      var last = getAndAdd(map.header, H_LAST, 16L);
      valueSegmentOf(last).set(last, getAndSet(segment, base + VALUE, value), getAndSet(segment, base + NEXT, last));
    }

    private void add(long value, LongBinaryOperator comparator) {
      var v = segment.get(JAVA_LONG, base + VALUE);
      var c = comparator.applyAsLong(value, v);
      if (c == 0) {
        return;
      }
      if (c < 0) {
        add(value);
      } else {
        var prevEntry = new SegmentEntry(node - base, segment);
        var prevNode = node;
        for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0; ) {
          var s = valueSegmentOf(n);
          v = s.value(n);
          if ((c = comparator.applyAsLong(value, v)) == 0) return;
          if (c < 0) {
            var last = getAndAdd(map.header, H_LAST, 16L);
            var lastEntry = valueSegmentOf(last);
            prevEntry.next(prevNode, last);
            lastEntry.set(last, value, n);
            return;
          } else {
            var nn = s.next(n);
            if (nn < 0L) {
              var last = getAndAdd(map.header, H_LAST, 16L);
              valueSegmentOf(last).set(last, value, -1L);
              s.next(n, last);
              return;
            }
            prevNode = n;
            prevEntry = s;
            n = nn;
          }
        }
      }
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
      return couldUseTheSameSegment(node, offset, NODE_SIZE, segmentSize)
        ? new VirtualNode(segment, node, (int) (node - offset), segmentSize, map)
        : map.newNode(node);
    }

    private SegmentEntry valueSegmentOf(long addr) {
      var offset = node - base;
      return couldUseTheSameSegment(addr, offset, 16, segmentSize)
        ? new SegmentEntry(offset, segment)
        : map.segment(addr, 16);
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
        var entry = valueSegmentOf(n);
        consumer.accept(entry.value(n));
        n = entry.next(n);
      }
    }

    private boolean contains(long value) {
      if (value == segment.get(JAVA_LONG, base + VALUE)) return true;
      for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0L; ) {
        var entry = valueSegmentOf(n);
        if (entry.value(n) == value) return true;
        n = entry.next(n);
      }
      return false;
    }

    private boolean contains(long value, LongBinaryOperator comparator) {
      var c = comparator.applyAsLong(value, segment.get(JAVA_LONG, base + VALUE));
      if (c == 0) return true;
      for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0L; ) {
        var entry = valueSegmentOf(n);
        if ((c = comparator.applyAsLong(value, entry.value(n))) == 0) return true;
        else if (c < 0) return false;
        else n = entry.next(n);
      }
      return false;
    }

    private long countValues() {
      long c = 1;
      for (var n = segment.get(JAVA_LONG, base + NEXT); n >= 0L; n = valueSegmentOf(n).next(n)) {
        c++;
      }
      return c;
    }

    private LongStream values() {
      return Stream.iterate(
        segment.asSlice(base + VALUE, 16L),
        Objects::nonNull,
        s -> {
          var n = s.get(JAVA_LONG, 8);
          return n < 0L ? null : valueSegmentOf(n).valueSegment(n);
        }
      ).mapToLong(s -> s.get(JAVA_LONG, 0));
    }
  }

  private record SegmentEntry(long offset, MemorySegment segment) {

    private MemorySegment valueSegment(long addr) {
      return segment.asSlice(addr - offset, 16L);
    }

    private long value(long addr) {
      return segment.get(JAVA_LONG, addr - offset);
    }

    private long next(long addr) {
      return segment.get(JAVA_LONG, addr - offset + 8);
    }

    private void next(long addr, long next) {
      segment.set(JAVA_LONG, addr - offset + 8, next);
    }

    private void set(long addr, long value, long next) {
      var o = addr - offset;
      segment.set(JAVA_LONG, o, value);
      segment.set(JAVA_LONG, o + 8, next);
    }
  }
}
