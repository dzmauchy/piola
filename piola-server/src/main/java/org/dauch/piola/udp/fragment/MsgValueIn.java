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
import java.util.function.Function;

import static org.dauch.piola.udp.UdpUtils.crc;

public final class MsgValueIn {

  public final long time = System.nanoTime();
  public final int parts;
  public final int checksum;
  public final int size;
  private final FastBitSet state;
  private ByteBuffer buffer;
  private volatile MessageStatus status = MessageStatus.NON_COMPLETED;

  public MsgValueIn(Fragment fragment) {
    this.parts = fragment.parts();
    this.checksum = fragment.checksum();
    this.size = fragment.size();
    this.state = new FastBitSet(parts);
  }

  public void apply(Fragment fragment, ByteBuffer buf, BufferManager manager) {
    if (fragment.parts() != parts)
      throw new DataCorruptionException("parts mismatch", null);
    if (fragment.checksum() != checksum)
      throw new DataCorruptionException("checksum mismatch", null);
    if (fragment.size() != size)
      throw new DataCorruptionException("size mismatch", null);
    synchronized (this) {
      if (state.get(fragment.part()))
        return;
      if (buffer == null) {
        if (state.nonEmpty())
          return;
        buffer = manager.get();
      }
      buffer.put(fragment.offset(), buf, buf.position(), buf.remaining());
      state.set(fragment.part());
    }
  }

  public synchronized MessageStatus tryComplete() {
    if (status == MessageStatus.NON_COMPLETED) {
      if (state.cardinality() == parts) {
        if (checksum == crc(buffer.position(0).limit(size))) {
          return status = MessageStatus.COMPLETED;
        } else {
          return status = MessageStatus.COMPLETED_WITH_ERROR;
        }
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public boolean isExpired(long time, long timeout) {
    return time - this.time > timeout;
  }

  public synchronized boolean release(BufferManager manager) {
    if (buffer != null) {
      manager.release(buffer);
      buffer = null;
      status = MessageStatus.TO_BE_CLEANED;
      return true;
    } else {
      return false;
    }
  }

  public synchronized <T> T withBuffer(Function<ByteBuffer, T> f) {
    return f.apply(buffer.position(0).limit(size));
  }

  public MessageStatus getStatus() {
    return status;
  }

  public synchronized void clear() {
    buffer = null;
  }
}
