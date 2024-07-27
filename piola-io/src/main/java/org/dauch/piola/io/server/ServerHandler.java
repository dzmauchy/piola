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

import org.dauch.piola.io.api.request.*;
import org.dauch.piola.io.api.response.*;
import org.dauch.piola.io.attributes.FileAttrs;
import org.dauch.piola.io.attributes.SimpleAttrs;
import org.dauch.piola.io.validation.TopicValidation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.lang.Integer.remainderUnsigned;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class ServerHandler implements AutoCloseable {

  private final System.Logger logger;
  private final Path baseDir;
  private final ReentrantLock[] locks = new ReentrantLock[256];
  private final ConcurrentSkipListMap<String, TopicData> topics = new ConcurrentSkipListMap<>();

  public ServerHandler(System.Logger logger, Path baseDir) throws Exception {
    this.logger = logger;
    this.baseDir = baseDir;
    Files.createDirectories(baseDir);
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  public void createTopic(TopicCreateRequest request, Consumer<? super Response> consumer) {
    withWriteLock(request.topic(), d -> {
      try {
        if (!d.exists()) {
          d.create();
        }
        var attrs = d.readFileAttrs();
        if (attrs == null) {
          throw new IllegalStateException("Unable to read attributes");
        }
        if (request.attrs() instanceof SimpleAttrs a && attrs instanceof FileAttrs fa) {
          fa.update(a);
        }
        consumer.accept(new TopicInfoResponse(request.topic(), attrs));
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to create topic in " + d, e);
        consumer.accept(new ErrorResponse("Topic creation error", e));
      }
    });
  }

  public void deleteTopic(TopicDeleteRequest request, Consumer<? super Response> consumer) {
    withWriteLock(request.topic(), d -> {
      try {
        if (d == null) {
          consumer.accept(new TopicNotFoundResponse());
          return;
        }
        if (d.exists()) {
          var fileAttrs = d.readFileAttrs();
          if (!(fileAttrs instanceof FileAttrs fa)) {
            consumer.accept(new TopicNotFoundResponse());
            return;
          }
          var attrs = fa.toSimpleAttrs();
          if (!d.delete()) {
            throw new IllegalStateException("Unable to delete the topic directory");
          }
          consumer.accept(new TopicInfoResponse(request.topic(), attrs));
        } else {
          consumer.accept(new TopicNotFoundResponse());
        }
        var old = topics.remove(request.topic());
        if (old != null) {
          logger.log(INFO, () -> "Closing " + old);
          old.close(logger);
        }
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to delete the topic " + request.topic(), e);
        consumer.accept(new ErrorResponse("Topic deletion error", e));
      }
    });
  }

  public void getTopic(TopicGetRequest request, Consumer<? super Response> consumer) {
    withReadLock(request.topic(), d -> {
      try {
        if (d == null) {
          consumer.accept(new TopicNotFoundResponse());
          return;
        }
        var fileAttrs = d.readFileAttrs();
        if (fileAttrs == null)
          throw new IllegalStateException("Unable to read attributes");
        consumer.accept(new TopicInfoResponse(request.topic(), fileAttrs));
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to get the topic " + request.topic(), e);
        consumer.accept(new ErrorResponse("Topic getting error", e));
      }
    });
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
        try (var fa = new FileAttrs(attrsFile, true)) {
          consumer.accept(new TopicInfoResponse(topic, fa.toSimpleAttrs()));
        }
      }
    }
    consumer.accept(new TopicInfoResponse("", null));
  }

  public void sendData(DataSendRequest request, ServerRequest sr, Consumer<? super Response> consumer) {
    TopicValidation.validateName(request.topic());
    withReadLock(request.topic(), d -> {
      if (d == null) {
        consumer.accept(new ErrorResponse("Topic " + request.topic() + " doesn't exist"));
      } else {
        var offset = d.writeData(sr.buffer(), request.labels());
        consumer.accept(new DataReceivedResponse(offset));
      }
    });
  }

  private void withWriteLock(String topic, Consumer<TopicData> task) {
    TopicValidation.validateName(topic);
    var locked = new AtomicBoolean(true);
    var lock = locks[remainderUnsigned(topic.hashCode(), locks.length)];
    lock.lock();
    try {
      var data = topics.computeIfAbsent(topic, _ -> new TopicData(baseDir.resolve(topic)));
      data.withWriteLock(() -> {
        lock.unlock();
        locked.set(false);
        task.accept(data);
      });
    } finally {
      if (locked.get()) {
        lock.unlock();
      }
    }
  }

  private void withReadLock(String topic, Consumer<TopicData> task) {
    TopicValidation.validateName(topic);
    var locked = new AtomicBoolean(true);
    var lock = locks[remainderUnsigned(topic.hashCode(), locks.length)];
    lock.lock();
    try {
      var data = topics.get(topic);
      if (data == null || !data.exists()) {
        lock.unlock();
        locked.set(false);
        task.accept(null);
      } else {
        data.withReadLock(() -> {
          lock.unlock();
          locked.set(false);
          task.accept(data);
        });
      }
    } finally {
      if (locked.get()) {
        lock.unlock();
      }
    }
  }

  @Override
  public void close() throws Exception {
    var closeException = new IllegalStateException("Close exception");
    topics.entrySet().removeIf(e -> {
      var topic = e.getKey();
      var data = e.getValue();
      try {
        logger.log(INFO, () -> "Closing topic data " + topic);
        data.close(logger);
      } catch (Throwable x) {
        closeException.addSuppressed(x);
      }
      return true;
    });
    if (closeException.getSuppressed().length > 0) {
      throw closeException;
    }
  }
}
