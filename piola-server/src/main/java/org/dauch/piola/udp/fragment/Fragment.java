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
import java.util.Objects;

public record Fragment(int size, int parts, int part, int len, int offset, int checksum) {

  public Fragment(ByteBuffer buf) {
    this(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt());
    if (size <= 0)
      throw new DataCorruptionException("size must be > 0", null);
    if (parts <= 0)
      throw new DataCorruptionException("Part count must be greater than zero", null);
    if (part < 0 || part >= parts)
      throw new DataCorruptionException("Part out of range", null);
    if (offset != part * len)
      throw new DataCorruptionException("offset out of range", null);
  }

  public void write(ByteBuffer buf) {
    buf.putInt(size);
    buf.putInt(parts);
    buf.putInt(part);
    buf.putInt(len);
    buf.putInt(offset);
    buf.putInt(checksum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, parts, part, len, offset, checksum);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Fragment that) {
      return size == that.size
        && parts == that.parts
        && part == that.part
        && len == that.len
        && offset == that.offset
        && checksum == that.checksum;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "Fragment(%d,%d,%d,%d,%d,%d)".formatted(size, parts, part, len, offset, checksum);
  }
}
