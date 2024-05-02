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
import org.dauch.piola.api.response.Response;
import org.dauch.piola.api.response.UnknownRequestResponse;
import org.dauch.piola.buffer.BufferManager;
import org.dauch.piola.util.*;

import java.math.BigInteger;
import java.nio.channels.ClosedChannelException;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.IntFunction;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.api.Constants.MAX_STREAMS;

public abstract class AbstractServer<RQ extends ServerRequest, RS extends ServerResponse> extends CompositeCloseable implements Server {

  protected final long exitCmd = ThreadLocalRandom.current().nextLong();

  protected final IntFunction<RQ[]> requestsArrayGenerator;
  protected final IntFunction<RS[]> responsesArrayGenerator;
  protected final int id;
  protected final ServerHandler handler;
  protected final BufferManager buffers;
  protected final BufferManager writeBuffers;
  protected final DrainQueue<RQ> requests;
  protected final DrainQueue<RS> responses;

  protected final BigIntCounter receivedRequests = new BigIntCounter();
  protected final BigIntCounter validRequests = new BigIntCounter();
  protected final BigIntCounter unknownRequests = new BigIntCounter();
  protected final BigIntCounter brokenRequests = new BigIntCounter();
  protected final BigIntCounter incompleteRequests = new BigIntCounter();
  protected final BigIntCounter sentMessages = new BigIntCounter();
  protected final BigIntCounter receivedSize = new BigIntCounter();
  protected final BigIntCounter sentSize = new BigIntCounter();
  protected final BigIntCounter unexpectedErrors = new BigIntCounter();

  protected final Thread mainLoopThread;
  protected final Thread requestThread;
  protected final Thread responseThread;

  protected volatile boolean runningRequests = true;
  protected volatile boolean runningResponses = true;
  protected volatile boolean running = true;

  private final AtomicReferenceArray<Thread> requestThreads = new AtomicReferenceArray<>(MAX_STREAMS);
  private final AtomicReferenceArray<Thread> responseThreads = new AtomicReferenceArray<>(MAX_STREAMS);

  protected AbstractServer(ServerConfig config, IntFunction<RQ[]> rqs, IntFunction<RS[]> rss) {
    super("Server[" + config.id() + "]");
    this.id = config.id();
    this.requestsArrayGenerator = rqs;
    this.responsesArrayGenerator = rss;
    try {
      buffers = $("read-buffers", new BufferManager("server-read", config));
      writeBuffers = $("write-buffers", new BufferManager("server-write", config));
      handler = $("handler", new ServerHandler(logger, config.baseDir()));
      requests = new DrainQueue<>(config.queueSize(), rqs);
      responses = new DrainQueue<>(config.queueSize(), rss);
      requestThread = Thread.ofVirtual().name("request-thread-" + config.id()).unstarted(this::requestLoop);
      responseThread = Thread.ofVirtual().name("response-thread-" + config.id()).unstarted(this::responseLoop);
      mainLoopThread = Thread.ofVirtual().name("server-loop-" + config.id()).unstarted(this::mainLoop);
    } catch (Throwable e) {
      throw initException(new IllegalStateException("Unable to initialize server", e));
    }
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public BigInteger getReceivedRequests() {
    return receivedRequests.get();
  }

  @Override
  public BigInteger getValidRequests() {
    return validRequests.get();
  }

  @Override
  public BigInteger getUnknownRequests() {
    return unknownRequests.get();
  }

  @Override
  public BigInteger getBrokenRequests() {
    return brokenRequests.get();
  }

  @Override
  public BigInteger getIncompleteRequests() {
    return incompleteRequests.get();
  }

  @Override
  public BigInteger getSentMessages() {
    return sentMessages.get();
  }

  @Override
  public BigInteger getReceivedSize() {
    return receivedSize.get();
  }

  @Override
  public BigInteger getSentSize() {
    return sentSize.get();
  }

  @Override
  public BigInteger getUnexpectedErrors() {
    return unexpectedErrors.get();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  protected abstract void requestLoop();
  protected abstract void responseLoop();
  protected abstract void doInMainLoop() throws Exception;

  private void mainLoop() {
    while (true) {
      try {
        doInMainLoop();
      } catch (ClosedChannelException | InterruptedException _) {
        logger.log(INFO, "Closed");
        break;
      } catch (Throwable e) {
        logger.log(ERROR, "Unexpected exception", e);
        unexpectedErrors.increment();
      }
    }
    logger.log(INFO, "Main loop finished");
  }


  protected void startThreads() {
    $("responses-thread", responseThread::join);
    $("responses", () -> {
      while (responses.nonEmpty()) {
        parkNanos(1_000_000L);
      }
      runningResponses = false;
    });
    $("requests-thread", requestThread::join);
    $("requests", () -> {
      while (requests.nonEmpty()) {
        parkNanos(1_000_000L);
      }
      runningRequests = false;
    });
    $("mainLoop", () -> {
      try {
        mainLoopThread.join();
      } finally {
        running = false;
      }
    });
    requestThread.start();
    responseThread.start();
    mainLoopThread.start();
  }

  protected final void doProcess(RQ element, Consumer<? super Response> responses) throws Exception {
    var request = element.request();
    switch (request) {
      case UnknownRequest r -> {
        unknownRequests.increment();
        responses.accept(new UnknownRequestResponse(r.code()));
      }
      case TopicCreateRequest r -> handler.createTopic(r, responses);
      case TopicDeleteRequest r -> handler.deleteTopic(r, responses);
      case TopicGetRequest r -> handler.getTopic(r, responses);
      case TopicListRequest r -> handler.listTopics(r, responses);
      case SendDataRequest r -> handler.sendData(r, responses);
    }
  }

  protected void drainRequests(Consumer<RQ> sink) {
    var rqs = requestsArrayGenerator.apply(requests.capacity());
    drain(requestThreads, requests.drain(rqs), rqs, sink);
  }

  protected void drainResponses(Consumer<RS> sink) {
    var rss = responsesArrayGenerator.apply(responses.capacity());
    drain(responseThreads, responses.drain(rss), rss, sink);
  }

  private <T extends Streamed> void drain(AtomicReferenceArray<Thread> ths, int count, T[] t, Consumer<T> sink) {
    if (count == 0) {
      parkNanos(1_000_000L);
      return;
    }
    var passed = new BitSet();
    for (int start = 0; start < count; ) {
      var hasNonVisited = false;
      for (int i = start; i < count; i++) {
        var e = t[i];
        if (e == null || passed.get(e.stream())) {
          if (!hasNonVisited) start++;
          continue;
        }
        var thread = sink(ths, e.stream(), i, count, t, sink);
        if (ths.compareAndSet(e.stream(), null, thread)) {
          thread.start();
          passed.set(e.stream());
          if (!hasNonVisited) start++;
        } else {
          hasNonVisited = true;
        }
      }
      if (hasNonVisited) {
        parkNanos(1_000_000L);
      } else {
        break;
      }
    }
  }

  private <T extends Streamed> Thread sink(AtomicReferenceArray<Thread> a, int s, int i, int count, T[] t, Consumer<T> sink) {
    return Thread.ofVirtual().unstarted(() -> {
      try {
        for (int k = i; k < count; k++) {
          var e = t[k];
          if (e.stream() == s) {
            sink.accept(e);
            t[k] = null; // free objects as soon as possible
          }
        }
      } catch (Throwable unexpectedError) {
        logger.log(ERROR, "Unexpected exception", unexpectedError);
      } finally {
        a.set(s, null);
      }
    });
  }
}
