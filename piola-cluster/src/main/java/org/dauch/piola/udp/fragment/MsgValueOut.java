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
import org.dauch.piola.util.FastBitSet;

public final class MsgValueOut {

  public final int parts;
  public final int checksum;
  public final int len;
  public final int size;
  private final FastBitSet state;

  public MsgValueOut(int parts, int checksum, int len, int size) {
    this.parts = parts;
    this.checksum = checksum;
    this.len = len;
    this.size = size;
    this.state = new FastBitSet(parts);
  }

  public synchronized boolean isCompleted() {
    return state.cardinality() == parts;
  }

  public void apply(Fragment fragment) {
    if (fragment.parts() != parts)
      throw new DataCorruptionException("parts mismatch", null);
    if (fragment.len() != len)
      throw new DataCorruptionException("len mismatch", null);
    if (fragment.checksum() != checksum)
      throw new DataCorruptionException("checksum mismatch", null);
    if (fragment.size() != size)
      throw new DataCorruptionException("size mismatch", null);
    if (fragment.part() < 0 || fragment.part() > parts)
      throw new DataCorruptionException("part of range", null);
    if (fragment.part() * len != fragment.offset())
      throw new DataCorruptionException("offset and part mismatch", null);
    synchronized (this) {
      state.set(fragment.part());
    }
  }
}
