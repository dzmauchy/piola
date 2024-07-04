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

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class FragmentCacheIn {

  private final ConcurrentSkipListMap<MsgKey, MsgValueIn> cache = new ConcurrentSkipListMap<>();

  public void clean(BufferManager manager, long timeout) {
    var time = System.nanoTime();
    cache.entrySet().removeIf(e -> {
      var v = e.getValue();
      if (v.isExpired(time, timeout)) {
        v.release(manager);
        return true;
      } else {
        return false;
      }
    });
  }

  public void reject(MsgKey key, BufferManager manager) {
    var v = cache.get(key);
    if (v != null) {
      v.release(manager);
    }
  }

  public MsgValueIn computeByKey(MsgKey key, Fragment fragment) {
    return cache.computeIfAbsent(key, _ -> new MsgValueIn(fragment));
  }

  public MsgValueIn getByKey(MsgKey key) {
    return cache.get(key);
  }

  public int close(BufferManager manager) {
    var counter = new AtomicInteger();
    cache.values().removeIf(v -> {
      if (v.release(manager)) {
        counter.getAndIncrement();
      }
      return true;
    });
    return counter.get();
  }
}
