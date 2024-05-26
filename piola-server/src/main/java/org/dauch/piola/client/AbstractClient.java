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

import org.dauch.piola.buffer.BufferManager;
import org.dauch.piola.util.CompositeCloseable;

import java.lang.ref.Cleaner;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Character.MAX_RADIX;
import static java.lang.System.Logger.Level.ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractClient extends CompositeCloseable implements Client {

  protected static final Cleaner CLEANER = Cleaner.create(Thread.ofVirtual().name("client-cleaner").factory());

  protected final long exitCmd = ThreadLocalRandom.current().nextLong();
  protected final AtomicLong ids = new AtomicLong();
  protected final ConcurrentSkipListMap<Long, LinkedTransferQueue<ClientResponse<?>>> responses = new ConcurrentSkipListMap<>();
  protected final BufferManager buffers;
  protected final BufferManager writeBuffers;
  protected final Thread responseScannerThread;

  protected AbstractClient(ClientConfig config) {
    super("client-" + config.name());
    try {
      var prefix = new BigInteger(1, config.name().getBytes(UTF_8)).toString(MAX_RADIX);
      buffers = $("read-buffer", new BufferManager(prefix + "-read", config));
      writeBuffers = $("write-buffer", new BufferManager(prefix + "-write", config));
      responseScannerThread = Thread.ofVirtual().name("response-scanner-" + config.name()).unstarted(this::scanResponses);
    } catch (Throwable e) {
      throw initException(new IllegalStateException("Unable to start " + logger.getName()));
    }
  }

  protected abstract void doScanResponses(ByteBuffer buffer) throws Exception;
  protected abstract void sendShutdownSequence() throws Exception;

  protected void startThreads() {
    $("mainLoopThread", responseScannerThread::join);
    $("mainLoop", this::sendShutdownSequence);
    responseScannerThread.start();
  }

  private void scanResponses() {
    while (true) {
      var buf = buffers.get();
      try {
        doScanResponses(buf);
      } catch (ClosedChannelException | InterruptedException e) {
        break;
      } catch (Throwable e) {
        logger.log(ERROR, "Unexpected error", e);
      } finally {
        buffers.release(buf);
      }
    }
  }
}
