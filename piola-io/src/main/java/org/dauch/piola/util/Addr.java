package org.dauch.piola.util;

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

import java.net.*;
import java.util.Arrays;

public record Addr(InetAddress address, int port) implements Comparable<Addr> {

  public Addr(InetSocketAddress address) {
    this(address.getAddress(), address.getPort());
  }

  @Override
  public int compareTo(Addr a) {
    int cmp = port - a.port;
    if (cmp != 0) return cmp;
    if (address instanceof Inet4Address a1 && a.address instanceof Inet4Address a2) {
      return a1.hashCode() - a2.hashCode();
    } else {
      return Arrays.compare(address.getAddress(), a.address.getAddress());
    }
  }

  @Override
  public String toString() {
    return address + ":" + port;
  }
}
