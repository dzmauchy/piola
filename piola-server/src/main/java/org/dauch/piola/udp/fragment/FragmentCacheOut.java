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

import java.util.concurrent.ConcurrentSkipListMap;

public final class FragmentCacheOut {

  private final ConcurrentSkipListMap<MsgKey, MsgValueOut> cache = new ConcurrentSkipListMap<>();

  public MsgValueOut computeIfAbsent(MsgKey key, int parts, int checksum, int len, int size) {
    return cache.computeIfAbsent(key, _ -> new MsgValueOut(parts, checksum, len, size));
  }

  public void apply(MsgKey key, Fragment fragment) {
    var value = cache.get(key);
    if (value != null) {
      value.apply(fragment);
    }
  }

  public void remove(MsgKey key) {
    cache.remove(key);
  }
}
