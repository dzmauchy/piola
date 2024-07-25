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
import org.dauch.piola.io.api.response.ErrorResponse;
import org.dauch.piola.io.api.response.Response;
import org.dauch.piola.collections.buffer.BufferManager;
import org.dauch.piola.concurrent.BigIntCounter;
import org.dauch.piola.concurrent.DrainQueue;
import org.dauch.piola.io.exception.ExceptionData;
import org.dauch.piola.util.*;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.*;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.invoke.VarHandle.acquireFence;
import static java.lang.invoke.VarHandle.releaseFence;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.io.api.Constants.MAX_STREAMS;

public abstract class AbstractServer<RQ extends ServerRequest, RS extends ServerResponse> extends CompositeCloseable implements Server {

  private static final VarHandle THREADS_VH = MethodHandles.arrayElementVarHandle(Thread[].class);

  protected final IntFunction<RQ[]> requestsArrayGenerator;
  protected final IntFunction<RS[]> responsesArrayGenerator;
  protected final int id;
  protected final ServerHandler handler;
  protected final BufferManager readBuffers;
  protected final BufferManager writeBuffers;
  protected final DrainQueue<RQ> requests;

  protected final BigIntCounter receivedRequests = new BigIntCounter();
  protected final BigIntCounter validRequests = new BigIntCounter();
  protected final BigIntCounter unknownRequests = new BigIntCounter();
  protected final BigIntCounter brokenRequests = new BigIntCounter();
  protected final BigIntCounter incompleteRequests = new BigIntCounter();
  protected final BigIntCounter incompleteResponses = new BigIntCounter();
  protected final BigIntCounter sentMessages = new BigIntCounter();
  protected final BigIntCounter receivedSize = new BigIntCounter();
  protected final BigIntCounter sentSize = new BigIntCounter();
  protected final BigIntCounter unexpectedErrors = new BigIntCounter();

  protected final Thread mainLoopThread;
  protected final Thread requestThread;

  protected volatile boolean runningRequests = true;
  protected volatile boolean running = true;

  private final Thread[] requestThreads = new Thread[MAX_STREAMS];

  protected AbstractServer(ServerConfig config, IntFunction<RQ[]> rqs, IntFunction<RS[]> rss) {
    super("Server[" + config.id() + "]");
    this.id = config.id();
    this.requestsArrayGenerator = rqs;
    this.responsesArrayGenerator = rss;
    try {
      writeBuffers = $("writeBuffers", new BufferManager("server-write", config));
      readBuffers = $("readBuffers", new BufferManager("server-read", config));
      handler = new ServerHandler(logger, config.baseDir());
      requests = new DrainQueue<>(config.queueSize(), rqs);
      requestThread = Thread.ofVirtual().name("request-thread-" + config.id()).unstarted(this::requestLoop);
      mainLoopThread = Thread.ofVirtual().name("server-loop-" + config.id()).unstarted(this::mainLoop);
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to initialize server", e));
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
  public BigInteger getIncompleteResponses() {
    return incompleteResponses.get();
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

  protected abstract void mainLoop();
  protected abstract void shutdown();

  protected void closeMainLoop() {
    running = false;
    shutdown();
    while (true) {
      int c = 0;
      for (int i = 0; i < requestThreads.length; i++) {
        acquireFence();
        var thread = requestThreads[i];
        if (thread != null) {
          c++;
          while (true) {
            try {
              thread.join();
              break;
            } catch (InterruptedException _) {
              logger.log(INFO, "Interrupted on joining thread " + i);
            }
          }
        }
      }
      if (c > 0) {
        logger.log(INFO, c + " threads were joined");
      } else {
        break;
      }
    }
  }

  protected void startThreads() {
    $("handler", handler);
    $("requests-thread", requestThread::join);
    $("requests", () -> requests.awaitEmpty(() -> runningRequests = false));
    $("mainLoop", this::closeMainLoop);
    requestThread.start();
    mainLoopThread.start();
  }

  protected final void doProcess(RQ element, BiConsumer<ByteBuffer, ? super Response> responses) throws Exception {
    var request = element.request();
    switch (request) {
      case UnknownRequest r -> {
        unknownRequests.increment();
        responses.accept(null, new ErrorResponse("Unknown request: " + r.code()));
      }
      case TopicCreateRequest r -> handler.createTopic(r, rs -> responses.accept(null, rs));
      case TopicDeleteRequest r -> handler.deleteTopic(r, rs -> responses.accept(null, rs));
      case TopicGetRequest r -> handler.getTopic(r, rs -> responses.accept(null, rs));
      case TopicListRequest r -> handler.listTopics(r, rs -> responses.accept(null, rs));
      case DataSendRequest r -> handler.sendData(r, element, rs -> responses.accept(null, rs));
    }
  }

  protected void drainRequests(Predicate<RQ> sink) {
    var rqs = requestsArrayGenerator.apply(requests.capacity());
    drain(requestThreads, requests.drain(rqs), rqs, sink);
  }

  protected abstract void writeResponse(RQ rq, ByteBuffer payload, Response rs) throws Exception;
  protected abstract void reject(RQ rq);

  protected boolean processRequest(RQ r) {
    try {
      doProcess(r, (b, rs) -> {
        try {
          writeResponse(r, b, rs);
        } catch (Throwable e) {
          throw new BreakException(e);
        }
      });
      return false;
    } catch (BreakException e) {
      logger.log(INFO, () -> "Write exception " + r, e.getCause());
      reject(r);
      return true;
    } catch (Throwable e) {
      logger.log(ERROR, () -> "Unable to process request " + r, e);
      try {
        writeResponse(r, null, new ErrorResponse("Unknown error", ExceptionData.from(e)));
        return false;
      } catch (Throwable x) {
        logger.log(INFO, () -> "Write exception " + r, x);
        reject(r);
        return true;
      }
    }
  }

  private void requestLoop() {
    while (runningRequests) {
      drainRequests(this::processRequest);
    }
  }

  private <T extends ServerRequest> void drain(Thread[] ths, int count, T[] t, Predicate<T> sink) {
    if (count == 0) {
      parkNanos(1_000_000L);
      return;
    }
    for (int start = 0; start < count; ) {
      var hasNonVisited = false;
      for (int i = start; i < count; i++) {
        acquireFence();
        var e = t[i];
        if (e == null) {
          if (!hasNonVisited) start++;
          continue;
        }
        var thread = sink(ths, i, count, t, sink);
        if (THREADS_VH.compareAndSet(ths, e.stream(), null, thread)) {
          thread.start();
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

  private <T extends ServerRequest> Thread sink(Thread[] a, int i, int count, T[] t, Predicate<T> sink) {
    var s = t[i].stream();
    var threadName = "requests-" + id + "-" + s;
    return Thread.ofVirtual().name(threadName).unstarted(() -> {
      try {
        for (int k = i; k < count; k++) {
          var e = t[k];
          if (e.stream() == s) {
            t[k] = null;
            releaseFence();
            if (sink.test(e)) {
              for (k++; k < count; k++) {
                e = t[k];
                t[k] = null;
                releaseFence();
                var buf = e.buffer();
                if (buf != null) {
                  readBuffers.release(buf);
                }
              }
            }
            var buf = e.buffer();
            if (buf != null) {
              readBuffers.release(buf);
            }
          }
        }
      } catch (Throwable unexpectedError) {
        logger.log(ERROR, "Unexpected exception", unexpectedError);
      } finally {
        a[s] = null;
        releaseFence();
      }
    });
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + id + ")";
  }

  private static final class BreakException extends RuntimeException {
    private BreakException(Throwable cause) {
      super(null, cause, false, false);
    }
  }
}
