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

import org.dauch.piola.collections.buffer.BufferManager;
import org.dauch.piola.udp.ServerClientConfig;
import org.dauch.piola.util.Addr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.InterruptedByTimeoutException;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.dauch.piola.udp.UdpUtils.*;

public final class OutputFragments {

  private final FragmentCacheOut out = new FragmentCacheOut();
  private final BufferManager buffers;
  private final long sendTimeout;
  private final long sleepTimeout;
  private final DatagramChannel channel;

  public OutputFragments(DatagramChannel channel, long sendTimeout, long sleep, BufferManager buffers) {
    this.channel = channel;
    this.sendTimeout = sendTimeout;
    this.sleepTimeout = sleep;
    this.buffers = buffers;
  }

  public OutputFragments(DatagramChannel channel, ServerClientConfig config, BufferManager buffers) {
    this(channel, config.messageAssemblyTimeoutNanos(), config.ackTimeoutNanos(), buffers);
  }

  public void handleAck(MsgKey key, Fragment fragment) {
    out.apply(key, fragment);
  }

  public void sendAck(int serverId, int protocolId, MsgKey key, Fragment fr, ByteBuffer buf, InetSocketAddress addr) throws IOException {
    buf.clear()
      .putInt(0)
      .putInt(serverId)
      .putInt(protocolId)
      .put(RT_ACK);
    key.write(buf);
    fr.write(buf);
    buf
      .flip()
      .putInt(0, crc(buf.slice(Integer.BYTES, buf.limit() - Integer.BYTES)));
    channel.send(buf, addr);
  }

  public int send(int partLen, int serverId, int protocolId, int stream, long id, ByteBuffer buffer, InetSocketAddress addr) throws IOException {
    var size = buffer.limit();
    var parts = size / partLen + (size % partLen == 0 ? 0 : 1);
    var key = new MsgKey(new Addr(addr), stream, id);
    var checksum = crc(buffer.slice(0, size));
    var value = out.computeIfAbsent(key, parts, checksum, partLen, size);
    var buf = buffers.get();
    try {
      int iter = 0;
      for (long t = System.nanoTime(); (System.nanoTime() - t) < sendTimeout; iter++) {
        for (int part = 0; part < parts; part++) {
          var offset = part * partLen;
          var fragment = new Fragment(size, parts, part, partLen, offset, checksum);
          buf.clear()
            .putInt(0)
            .putInt(protocolId)
            .putInt(serverId)
            .put(RT_FRAGMENT);
          key.write(buf);
          fragment.write(buf);
          var len = Math.min(size - offset, partLen);
          buf
            .put(buffer.slice(offset, len))
            .flip()
            .putInt(0, crc(buf.slice(Integer.BYTES, buf.limit() - Integer.BYTES)));
          channel.send(buf, addr);
          parkNanos(sleepTimeout);
          if (iter > 0 && value.isCompleted()) {
            return iter;
          }
        }
      }
      throw new InterruptedByTimeoutException();
    } finally {
      out.remove(key);
      buffers.release(buf);
    }
  }
}
