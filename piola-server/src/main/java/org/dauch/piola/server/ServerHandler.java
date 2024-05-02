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
import org.dauch.piola.attributes.Attributes;
import org.dauch.piola.attributes.FileAttrs;
import org.dauch.piola.exception.ExceptionData;
import org.dauch.piola.validation.TopicValidation;

import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static java.lang.System.Logger.Level.ERROR;
import static org.dauch.piola.api.attributes.TopicFileAttrs.ALL_TOPIC_ATTRS;

public final class ServerHandler implements AutoCloseable {

  private final System.Logger logger;
  private final Path baseDir;
  private final ConcurrentHashMap<String, TopicData> topics = new ConcurrentHashMap<>(64, 0.5f, 16);

  public ServerHandler(System.Logger logger, Path baseDir) throws Exception {
    this.logger = logger;
    this.baseDir = baseDir;
    Files.createDirectories(baseDir);
  }

  private TopicData byTopic(String topic) {
    return new TopicData(baseDir.resolve(topic));
  }

  public void createTopic(TopicCreateRequest request, Consumer<? super Response> consumer) {
    TopicValidation.validate(request);
    consumer.accept(topics.computeIfAbsent(request.topic(), this::byTopic).withWriteLock(d -> {
      try {
        d.create();
        var attrs = d.attributes();
        request.attrs().writeTo(attrs);
        return new TopicDataResponse(request.topic(), request.attrs());
      } catch (FileAlreadyExistsException _) {
        return new TopicAlreadyExistsResponse();
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to create topic in " + d, e);
        return new ErrorResponse("Topic creation error", ExceptionData.from(e));
      }
    }));
  }

  public void deleteTopic(TopicDeleteRequest request, Consumer<? super Response> consumer) {
    TopicValidation.validate(request);
    consumer.accept(topics.computeIfAbsent(request.topic(), this::byTopic).withWriteLock(d -> {
      try {
        if (d.exists()) {
          var attrs = d.attributes();
          var fileAttrs = new FileAttrs(attrs, ALL_TOPIC_ATTRS);
          return d.delete() ? new TopicDataResponse(request.topic(), fileAttrs) : new TopicNotFoundResponse();
        }
        return d.delete() ? new TopicDataResponse(request.topic(), new FileAttrs()) : new TopicNotFoundResponse();
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to delete the topic " + request.topic(), e);
        return new ErrorResponse("Topic deletion error", ExceptionData.from(e));
      }
    }));
  }

  public void getTopic(TopicGetRequest request, Consumer<? super Response> consumer) {
    TopicValidation.validate(request);
    consumer.accept(topics.computeIfAbsent(request.topic(), this::byTopic).withReadLock(d -> {
      try {
        if (d.exists()) {
          var attrs = d.attributes();
          var fileAttrs = new FileAttrs(attrs, ALL_TOPIC_ATTRS);
          return new TopicDataResponse(request.topic(), fileAttrs);
        }
        return new TopicNotFoundResponse();
      } catch (Throwable e) {
        logger.log(ERROR, () -> "Unable to delete the topic " + request.topic(), e);
        return new ErrorResponse("Topic deletion error", ExceptionData.from(e));
      }
    }));
  }

  public void listTopics(TopicListRequest request, Consumer<? super Response> consumer) throws Exception {
    var buf = ByteBuffer.allocate(256);
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
        var attrs = new Attributes(dir, Files.getFileAttributeView(dir, UserDefinedFileAttributeView.class), buf);
        var fileAttrs = new FileAttrs(attrs, ALL_TOPIC_ATTRS);
        consumer.accept(new TopicDataResponse(topic, fileAttrs));
      }
    }
    consumer.accept(new TopicDataResponse("", null));
  }

  public void sendData(SendDataRequest request, Consumer<? super Response> consumer) {
  }

  @Override
  public void close() throws Exception {
  }
}
