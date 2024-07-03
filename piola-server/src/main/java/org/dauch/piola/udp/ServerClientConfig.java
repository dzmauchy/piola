package org.dauch.piola.udp;

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

import org.dauch.piola.collections.buffer.BufferConfig;
import org.dauch.piola.collections.buffer.BufferManager;

import java.util.concurrent.TimeUnit;

public interface ServerClientConfig extends BufferConfig {

  int rcvBufSize();
  int sendBufSize();
  int multicastTtl();
  boolean multicastLoop();
  int messageAssemblyTimeout();
  int ackTimeout();
  int maxFragmentSize();
  int fragmentBufferCount();

  default BufferManager fragmentBuffers(String suffix) {
    return new BufferManager("udp-" + suffix, bufferDir(), fragmentBufferCount(), maxFragmentSize(), 0.5f, sparse());
  }

  default long messageAssemblyTimeoutNanos() {
    return TimeUnit.MILLISECONDS.toNanos(messageAssemblyTimeout());
  }

  default long ackTimeoutNanos() {
    return TimeUnit.MILLISECONDS.toNanos(ackTimeout());
  }
}
