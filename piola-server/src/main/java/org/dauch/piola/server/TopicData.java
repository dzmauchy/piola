package org.dauch.piola.server;

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

import org.dauch.piola.attributes.FileAttrs;
import org.dauch.piola.util.MoreFiles;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class TopicData implements AutoCloseable {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Path directory;
  private Arena arena;
  private FileAttrs attrs;

  TopicData(Path directory) {
    this.directory = directory;
  }

  FileAttrs readFileAttrs() {
    if (attrs != null) {
      return attrs;
    } else {
      if (arena == null) {
        arena = Arena.ofShared();
      }
      return attrs = new FileAttrs(directory.resolve("attributes.data"), arena, false);
    }
  }

  boolean delete() throws IOException {
    if (MoreFiles.deleteRecursively(directory)) {
      if (arena instanceof Arena a) {
        try (a) {
          attrs = null;
        }
        arena = null;
      }
      return true;
    } else {
      return false;
    }
  }

  boolean exists() {
    return Files.exists(directory);
  }

  void create() throws Exception {
    Files.createDirectory(directory);
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

  @Override
  public void close() {
    if (arena != null) {
      arena.close();
    }
  }

  @Override
  public String toString() {
    return directory.getFileName().toString();
  }
}
