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

import org.dauch.piola.io.attributes.*;
import org.dauch.piola.util.MoreFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNullElseGet;

final class TopicData {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final Path directory;
  private FileAttrs attrs;

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

  public void close() {
    if (attrs != null) {
      attrs.close();
    }
  }

  @Override
  public String toString() {
    return directory.getFileName().toString();
  }
}
