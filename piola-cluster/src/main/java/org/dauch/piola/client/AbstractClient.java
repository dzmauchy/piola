package org.dauch.piola.client;

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

import org.dauch.piola.api.ResponseFactory;
import org.dauch.piola.api.SerializationContext;
import org.dauch.piola.api.request.Request;
import org.dauch.piola.api.response.*;
import org.dauch.piola.collections.buffer.BufferManager;
import org.dauch.piola.util.*;

import java.lang.ref.Cleaner;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Character.MAX_RADIX;
import static java.lang.System.Logger.Level.WARNING;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractClient extends CompositeCloseable implements Client {

  protected static final Cleaner CLEANER = Cleaner.create(Thread.ofVirtual().name("client-cleaner").factory());

  protected final String name;
  protected final AtomicLong ids = new AtomicLong();
  protected final ConcurrentSkipListMap<Long, LinkedTransferQueue<ClientResponse<?>>> responses = new ConcurrentSkipListMap<>();
  protected final BufferManager buffers;
  protected final BufferManager writeBuffers;
  protected final Thread mainLoopThread;

  protected final BigIntCounter brokenResponses = new BigIntCounter();
  protected final BigIntCounter incompleteResponses = new BigIntCounter();
  protected final BigIntCounter receivedResponses = new BigIntCounter();
  protected final BigIntCounter receivedSize = new BigIntCounter();
  protected final BigIntCounter sentRequests = new BigIntCounter();
  protected final BigIntCounter sentSize = new BigIntCounter();
  protected final BigIntCounter unexpectedErrors = new BigIntCounter();
  protected final BigIntCounter unknownResponses = new BigIntCounter();
  protected final BigIntCounter errorResponses = new BigIntCounter();
  protected final BigIntCounter forgottenResponses = new BigIntCounter();

  protected volatile boolean running = true;

  protected AbstractClient(ClientConfig config) {
    super("client-" + config.name());
    try {
      name = config.name();
      var prefix = new BigInteger(1, config.name().getBytes(UTF_8)).toString(MAX_RADIX);
      buffers = $("read-buffer", new BufferManager(prefix + "-read", config));
      writeBuffers = $("write-buffer", new BufferManager(prefix + "-write", config));
      mainLoopThread = Thread.ofVirtual().name("responses-" + config.name()).unstarted(this::scanResponses);
    } catch (Throwable e) {
      throw constructorException(new IllegalStateException("Unable to start " + logger.getName()));
    }
  }

  protected abstract void scanResponses();
  protected abstract void fill(ByteBuffer buf, long id, Request<?> rq, ByteBuffer payload);
  protected abstract int send(ByteBuffer buf, Request<?> rq, int stream, long id, InetSocketAddress address) throws Exception;
  protected abstract InetSocketAddress sendShutdownSequence();

  protected void closeMainLoop() {
    running = false;
    Threads.close(mainLoopThread, this::sendShutdownSequence, logger);
  }

  protected void startThreads() {
    $("mainLoop", this::closeMainLoop);
    mainLoopThread.start();
  }

  private <RS extends Response> SimpleResponses<RS> responses() {
    var id = ids.getAndIncrement();
    var responses = this.responses;
    var closer = (Runnable) () -> responses.remove(id);
    var fetcher = new SimpleResponses<RS>(id, responses.computeIfAbsent(id, _ -> new LinkedTransferQueue<>()), closer);
    CLEANER.register(fetcher, closer);
    return fetcher;
  }

  @Override
  public final <RQ extends Request<RS>, RS extends Response> Responses<RS> send(RQ request, ByteBuffer payload, int stream, InetSocketAddress... addresses) {
    var fetcher = this.<RS>responses();
    var buf = writeBuffers.get();
    try {
      fill(buf, fetcher.id, request, payload);
      for (var addr : addresses) {
        var b = buf.slice(0, buf.position());
        try {
          send(b, request, stream, fetcher.id, addr);
        } catch (Throwable e) {
          logger.log(WARNING, "Error", e);
          fetcher.putError(addr, e);
        }
      }
      return fetcher;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      writeBuffers.release(buf);
    }
  }

  @Override
  public BigInteger getBrokenResponses() {
    return brokenResponses.get();
  }

  @Override
  public BigInteger getIncompleteResponses() {
    return incompleteResponses.get();
  }

  @Override
  public BigInteger getReceivedResponses() {
    return receivedResponses.get();
  }

  @Override
  public BigInteger getReceivedSize() {
    return receivedSize.get();
  }

  @Override
  public BigInteger getSentRequests() {
    return sentRequests.get();
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
  public BigInteger getUnknownResponses() {
    return unknownResponses.get();
  }

  @Override
  public BigInteger getErrorResponses() {
    return errorResponses.get();
  }

  @Override
  public BigInteger getForgottenResponses() {
    return forgottenResponses.get();
  }

  protected ClientResponse<?> clientResponse(ByteBuffer buffer, int protocolId, int serverId, int stream, InetSocketAddress addr) {
    var in = new SerializationContext();
    var response = ResponseFactory.read(buffer, in);
    switch (response) {
      case ErrorResponse _ -> errorResponses.increment();
      case UnknownResponse _ -> unknownResponses.increment();
      default -> {}
    }
    var out = SerializationContext.read(buffer);
    var remaining = buffer.remaining();
    final byte[] payload;
    if (remaining > 0) {
      payload = new byte[remaining];
      buffer.get(payload);
    } else {
      payload = null;
    }
    return new ClientResponse<>(protocolId, serverId, stream, addr, response, out, in, payload);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + name + ")";
  }
}
