package org.dauch.piola.io.server;

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

import org.dauch.piola.collections.map.LongLongAVLDiskMap;
import org.dauch.piola.io.attributes.*;
import org.dauch.piola.util.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.System.Logger.Level.ERROR;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Objects.requireNonNullElseGet;

final class TopicData {

  private final ByteBuffer attrBuffer = ByteBuffer.allocateDirect(2 << 17);
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final TreeMap<Long, LongLongAVLDiskMap> indices = new TreeMap<>();
  private final Path directory;
  private FileAttrs attrs;
  private FileChannel dataChannel;

  TopicData(Path directory) {
    this.directory = directory;
  }

  Attrs readFileAttrs() {
    if (Files.isDirectory(directory)) {
      return requireNonNullElseGet(attrs, () -> attrs = new FileAttrs(directory.resolve("attributes.data"), false));
    } else {
      return EmptyAttrs.EMPTY_ATTRS;
    }
  }

  boolean delete() throws IOException {
    if (MoreFiles.deleteRecursively(directory)) {
      attrs.close();
      attrs = null;
      return true;
    } else {
      return false;
    }
  }

  boolean exists() {
    return Files.exists(directory);
  }

  void create() throws Exception {
    createDirectories(directory.resolve("index"));
  }

  void withReadLock(Runnable task) {
    lock.readLock().lock();
    try {
      task.run();
    } finally {
      lock.readLock().unlock();
    }
  }

  void withWriteLock(Runnable task) {
    lock.writeLock().lock();
    try {
      task.run();
    } finally {
      lock.writeLock().unlock();
    }
  }

  synchronized long writeData(ByteBuffer buffer, Attrs attributes) {
    try {
      if (dataChannel == null) {
        dataChannel = FileChannel.open(directory.resolve("data.data"), EnumSet.of(CREATE, WRITE, APPEND));
      }
      var pos = dataChannel.position();
      attributes.write(attrBuffer.clear().putInt(buffer.remaining()));
      var b = new ByteBuffer[]{attrBuffer.flip(), buffer};
      while (attrBuffer.hasRemaining() && buffer.hasRemaining()) {
        var n = dataChannel.write(b);
        if (n < 0) {
          throw new EOFException();
        }
      }
      for (int i = 0, l = attributes.size(); i < l; i++) {
        getOrCreateIndex(attributes.getKeyByIndex(i)).put(attributes.getValueByIndex(i), pos);
      }
      return pos;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private LongLongAVLDiskMap getOrCreateIndex(long key) {
    return indices.computeIfAbsent(key, k -> {
      var file = directory.resolve("index").resolve(Id.encode(k));
      return new LongLongAVLDiskMap(file, 1 << 20, 64);
    });
  }

  public void close(System.Logger logger) {
    try (var _ = attrs; var _ = dataChannel; var cc = new CompositeCloseable(logger)) {
      indices.forEach((k, v) -> cc.add(Id.encode(k), v));
      indices.clear();
    } catch (Throwable e) {
      logger.log(ERROR, "Unable to close", e);
    }
  }

  @Override
  public String toString() {
    return directory.getFileName().toString();
  }
}
