package org.dauch.piola.udp.fragment;

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
import org.dauch.piola.exception.DataCorruptionException;
import org.dauch.piola.util.FastBitSet;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.dauch.piola.udp.UdpUtils.crc;

public final class MsgValue {

  private static final AtomicIntegerFieldUpdater<MsgValue> COMPLETED = newUpdater(MsgValue.class, "completed");
  private static final AtomicReferenceFieldUpdater<MsgValue, ByteBuffer> BUFFER = newUpdater(MsgValue.class, ByteBuffer.class, "buffer");

  public final long startTime = System.nanoTime();
  public final int parts;
  public final int checksum;
  public final int size;
  private final FastBitSet state;
  private final FastBitSet remoteState;
  private volatile ByteBuffer buffer;
  private volatile int completed;

  public MsgValue(Fragment fragment) {
    this.parts = fragment.parts();
    this.checksum = fragment.checksum();
    this.size = fragment.size();
    this.state = new FastBitSet(parts);
    this.remoteState = new FastBitSet(parts);
  }

  public void validate(Fragment fragment) {
    if (fragment.parts() != parts)
      throw new DataCorruptionException("parts mismatch", null);
    if (fragment.checksum() != checksum)
      throw new DataCorruptionException("checksum mismatch", null);
    if (fragment.size() != size)
      throw new DataCorruptionException("size mismatch", null);
  }

  public synchronized void apply(Fragment fragment, ByteBuffer buf, Supplier<ByteBuffer> bufferSupplier) {
    if (buf.remaining() != fragment.len())
      throw new DataCorruptionException("Buffer length mismatch", null);
    if (buffer == null) {
      buffer = bufferSupplier.get();
      if (fragment.size() > buffer.remaining())
        throw new DataCorruptionException("Not enough space in buffer", null);
    }
    buffer.put(fragment.offset(), buf, buf.position(), fragment.len());
    state.set(fragment.part());
  }

  public synchronized void applyRemote(Fragment fragment) {
    remoteState.set(fragment.part());
  }

  public ByteBuffer slice() {
    return buffer.slice(0, size);
  }

  public boolean tryComplete() {
    return isCompleted() && checksum == crc(slice()) && COMPLETED.compareAndSet(this, 0, 1);
  }

  public synchronized boolean isCompleted() {
    return state.cardinality() == parts && remoteState.cardinality() == parts;
  }

  public boolean isExpired(long time, long timeout) {
    return time - startTime > timeout;
  }

  public synchronized boolean couldBeCleaned() {
    return state.nonEmpty() && buffer == null;
  }

  public void prepareAck(Fragment fragment, ByteBuffer buf) {
    buf.clear().putInt(0);
    fragment.write(buf);
    state.write(buf);
    remoteState.write(buf);
    buf.put((byte) completed);
    buf.putInt(0, crc(buf.slice(4, buf.position() - 4))).flip();
  }

  public void release(BufferManager manager) {
    var b = buffer;
    if (BUFFER.compareAndSet(this, b, null)) {
      manager.release(b);
    }
  }

  public <T> T withRawBuffer(Function<ByteBuffer, T> func) {
    return func.apply(buffer);
  }
}
