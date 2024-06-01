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

import org.dauch.piola.exception.DataCorruptionException;

import java.nio.ByteBuffer;

public record MsgKey(int stream, long millis, long rnd, long id) implements Comparable<MsgKey> {

  public MsgKey(ByteBuffer buffer) {
    this(buffer.getInt(), buffer.getLong(), buffer.getLong(), buffer.getLong());
    if (stream < 0 || stream >= Character.MAX_VALUE)
      throw new DataCorruptionException("stream out of bounds", null);
    if (millis < 0L)
      throw new DataCorruptionException("millis out of bounds", null);
  }

  @Override
  public int compareTo(MsgKey that) {
    var cmp = stream - that.stream;
    if (cmp != 0)
      return cmp;
    cmp = Long.compare(millis, that.millis);
    if (cmp != 0)
      return cmp;
    cmp = Long.compare(rnd, that.rnd);
    if (cmp != 0)
      return cmp;
    return Long.compare(id, that.id);
  }

  public void write(ByteBuffer buf) {
    buf.putInt(stream).putLong(millis).putLong(rnd).putLong(id);
  }

  @Override
  public String toString() {
    return "Key(%d,%d,%d,%d)".formatted(stream, millis, rnd, id);
  }
}
