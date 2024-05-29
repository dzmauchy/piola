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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.*;

final class TopicData {

  private final Path directory;
  private final FileSystemProvider provider;
  private FileAttrs attrs;
  final String topic;
  TopicData next;

  TopicData(Path directory, String topic) {
    this.directory = directory;
    this.provider = directory.getFileSystem().provider();
    this.topic = topic;
  }

  FileAttrs readFileAttrs() throws Exception {
    if (attrs != null) {
      return attrs;
    }
    var attrsFile = directory.resolve("attributes.data");
    if (provider.exists(attrsFile)) {
      try (var ch = open(attrsFile, EnumSet.of(READ, WRITE))) {
        return attrs = new FileAttrs(ch.map(READ_WRITE, 0L, ch.size()));
      } catch (NoSuchFileException _) {
        return null;
      }
    } else if (provider.exists(directory)) {
      try (var ch = open(attrsFile, EnumSet.of(READ, WRITE, CREATE_NEW, SPARSE))) {
        return attrs = new FileAttrs(ch.map(READ_WRITE, 0L, 1L << 16));
      }
    }
    return null;
  }

  boolean delete() throws IOException {
    if (MoreFiles.deleteRecursively(directory)) {
      attrs = null;
      return true;
    } else {
      return false;
    }
  }

  boolean exists() {
    return provider.exists(directory);
  }

  void create() throws Exception {
    provider.createDirectory(directory);
  }

  @Override
  public String toString() {
    return topic;
  }
}
