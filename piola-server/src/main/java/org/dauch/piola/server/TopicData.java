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

import org.dauch.piola.attributes.Attributes;
import org.dauch.piola.util.MoreFiles;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

final class TopicData {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ByteBuffer buffer = ByteBuffer.allocateDirect(256);

  private final Path directory;
  private final FileSystemProvider provider;
  private final UserDefinedFileAttributeView attrView;

  TopicData(Path dir) {
    directory = dir;
    provider = dir.getFileSystem().provider();
    attrView = provider.getFileAttributeView(dir, UserDefinedFileAttributeView.class);
  }

  Attributes attributes() {
    return new Attributes(directory, attrView, buffer);
  }

  void create() throws IOException {
    provider.createDirectory(directory);
  }

  boolean exists() {
    return provider.exists(directory);
  }

  boolean delete() throws IOException {
    return MoreFiles.deleteRecursively(directory);
  }

  <T> T withWriteLock(Function<TopicData, T> supplier) {
    lock.writeLock().lock();
    try {
      return supplier.apply(this);
    } finally {
      lock.writeLock().unlock();
    }
  }

  <T> T withReadLock(Function<TopicData, T> supplier) {
    lock.readLock().lock();
    try {
      return supplier.apply(this);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return directory.toString();
  }
}
