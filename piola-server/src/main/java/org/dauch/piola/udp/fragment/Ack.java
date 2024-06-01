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

import org.dauch.piola.udp.UdpUtils;
import org.dauch.piola.util.FastBitSet;

import java.nio.ByteBuffer;

public final class Ack {

  public final Fragment fragment;
  public final FastBitSet state;
  public final FastBitSet remoteState;
  public final boolean completed;

  public Ack(ByteBuffer buffer) {
    UdpUtils.validateCrc(buffer);
    fragment = new Fragment(buffer);
    state = new FastBitSet(buffer);
    remoteState = new FastBitSet(buffer);
    completed = buffer.get() != 0;
  }
}
