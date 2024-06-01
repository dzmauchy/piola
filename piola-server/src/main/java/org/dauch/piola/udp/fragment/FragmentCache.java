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

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class FragmentCache {

  private final ConcurrentSkipListMap<InetSocketAddress, ConcurrentSkipListMap<MsgKey, MsgValue>> cache =
    new ConcurrentSkipListMap<>(FragmentCache::addressCompare);

  public ConcurrentSkipListMap<MsgKey, MsgValue> fragmentsBy(InetSocketAddress address) {
    return cache.computeIfAbsent(address, _ -> new ConcurrentSkipListMap<>());
  }

  private static int addressCompare(InetSocketAddress a1, InetSocketAddress a2) {
    if (a1 == a2) return 0;
    var cmp = a1.getPort() - a2.getPort();
    if (cmp != 0) return cmp;
    var ia1 = a1.getAddress();
    var ia2 = a2.getAddress();
    if (ia1 instanceof Inet4Address && ia2 instanceof Inet4Address) {
      return ia1.hashCode() - ia2.hashCode();
    } else {
      var ab1 = ia1.getAddress();
      var ab2 = ia2.getAddress();
      return Arrays.compare(ab1, ab2);
    }
  }

  public int clean(BufferManager manager, long timeout) {
    var time = System.nanoTime();
    var removedEntries = new LinkedList<Entry<InetSocketAddress, ConcurrentSkipListMap<MsgKey, MsgValue>>>();
    var removedValues = new LinkedList<MsgValue>();
    var counter = new AtomicInteger();
    cache.entrySet().removeIf(e -> {
      var map = e.getValue();
      map.values().removeIf(v -> {
        if (v.isExpired(time, timeout)) {
          counter.getAndIncrement();
          return removedValues.add(v);
        } else {
          return v.couldBeCleaned();
        }
      });
      return map.isEmpty() && removedEntries.add(e);
    });
    removedValues.forEach(v -> v.release(manager));
    removedEntries.forEach(e -> {
      var map = e.getValue();
      if (!map.isEmpty()) {
        cache.compute(e.getKey(), (_, o) -> {
          if (o == null) {
            return map;
          } else {
            o.putAll(map);
            return o;
          }
        });
      }
    });
    return counter.get();
  }
}
