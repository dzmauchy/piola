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

import org.dauch.piola.api.request.*;
import org.dauch.piola.api.response.*;
import org.dauch.piola.attributes.FileAttrs;
import org.dauch.piola.attributes.SimpleAttrs;
import org.dauch.piola.exception.ExceptionData;
import org.dauch.piola.validation.TopicValidation;

import java.nio.file.*;
import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.System.Logger.Level.ERROR;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.READ;

public final class ServerHandler implements AutoCloseable {

  private final System.Logger logger;
  private final Path baseDir;
  private final ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[128];
  private final TopicData[] topics = new TopicData[locks.length];

  public ServerHandler(System.Logger logger, Path baseDir) throws Exception {
    this.logger = logger;
    this.baseDir = baseDir;
    Files.createDirectories(baseDir);
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  public void createTopic(TopicCreateRequest request, Consumer<? super Response> consumer) {
    consumer.accept(withWriteLock(request.topic(), d -> {
      try {
        if (!d.exists()) {
          d.create();
        }
        var attrs = d.readFileAttrs();
        if (attrs == null)
          throw new IllegalStateException("Unable to read attributes");
        if (request.attrs() instanceof SimpleAttrs a)
          attrs.update(a);
        return new TopicResponse(request.topic(), attrs);
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to create topic in " + d, e);
        return new ErrorResponse("Topic creation error", ExceptionData.from(e));
      }
    }));
  }

  public void deleteTopic(TopicDeleteRequest request, Consumer<? super Response> consumer) {
    consumer.accept(withWriteLock(request.topic(), d -> {
      try {
        if (d.exists()) {
          var fileAttrs = d.readFileAttrs();
          if (fileAttrs == null)
            throw new IllegalStateException("Unable to read attributes");
          var attrs = new SimpleAttrs(fileAttrs);
          if (!d.delete()) {
            throw new IllegalStateException("Unable to delete the topic directory");
          }
          return new TopicResponse(request.topic(), attrs);
        } else {
          return new TopicNotFoundResponse();
        }
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to delete the topic " + request.topic(), e);
        return new ErrorResponse("Topic deletion error", ExceptionData.from(e));
      } finally {
        removeTopicData(request.topic());
      }
    }));
  }

  public void getTopic(TopicGetRequest request, Consumer<? super Response> consumer) {
    consumer.accept(withReadLock(request.topic(), d -> {
      try {
        if (d == null)
          return new TopicNotFoundResponse();
        var fileAttrs = d.readFileAttrs();
        if (fileAttrs == null)
          throw new IllegalStateException("Unable to read attributes");
        return new TopicResponse(request.topic(), fileAttrs);
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to get the topic " + request.topic(), e);
        return new ErrorResponse("Topic getting error", ExceptionData.from(e));
      }
    }));
  }

  public void listTopics(TopicListRequest request, Consumer<? super Response> consumer) throws Exception {
    try (var ds = Files.newDirectoryStream(baseDir, Files::isDirectory)) {
      for (var dir : ds) {
        var topic = dir.getFileName().toString();
        if (!request.prefix().isEmpty()) {
          if (!topic.startsWith(request.prefix())) {
            continue;
          }
        }
        if (!request.suffix().isEmpty()) {
          if (!topic.endsWith(request.suffix())) {
            continue;
          }
        }
        if (!request.substring().isEmpty()) {
          if (!topic.contains(request.substring())) {
            continue;
          }
        }
        var attrsFile = dir.resolve("attributes.data");
        try (var ch = open(attrsFile, EnumSet.of(READ))) {
          var buffer = ch.map(READ_ONLY, 0L, ch.size());
          consumer.accept(new TopicResponse(topic, new FileAttrs(buffer)));
        } catch (NoSuchFileException _) {
        }
      }
    }
    consumer.accept(new TopicResponse("", null));
  }

  public void addData(SendDataRequest request, ServerRequest sr, Consumer<? super Response> consumer) {
  }

  private <T> T withWriteLock(String topic, Function<TopicData, T> f) {
    var index = Integer.remainderUnsigned(topic.hashCode(), locks.length);
    TopicValidation.validateName(topic);
    var dir = baseDir.resolve(topic);
    var lock = locks[index];
    lock.writeLock().lock();
    try {
      for (var d = topics[index]; d != null; d = d.next) {
        if (d.topic.equals(topic)) {
          return f.apply(d);
        }
        if (d.next == null) {
          return f.apply(d.next = new TopicData(dir, topic));
        }
      }
      return f.apply(topics[index] = new TopicData(dir, topic));
    } finally {
      lock.writeLock().unlock();
    }
  }

  private <T> T withReadLock(String topic, Function<TopicData, T> f) {
    var index = Integer.remainderUnsigned(topic.hashCode(), locks.length);
    TopicValidation.validateName(topic);
    var lock = locks[index];
    lock.readLock().lock();
    try {
      for (var d = topics[index]; d != null; d = d.next) {
        if (d.topic.equals(topic)) {
          return f.apply(d);
        }
      }
      return f.apply(null);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void removeTopicData(String topic) {
    var index = Integer.remainderUnsigned(topic.hashCode(), locks.length);
    var d = topics[index];
    if (d.topic.equals(topic)) {
      topics[index] = d.next;
      return;
    }
    for (; ; d = d.next) {
      if (d.next.topic.equals(topic)) {
        d.next = d.next.next;
        return;
      }
    }
  }

  @Override
  public void close() throws Exception {
  }
}
