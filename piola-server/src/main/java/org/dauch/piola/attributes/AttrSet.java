package org.dauch.piola.attributes;

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

import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

public final class AttrSet {

  private final FileAttr<?>[] attrs;

  public AttrSet(FileAttr<?>... attrs) {
    this.attrs = attrs;
  }

  public void forEachKey(Consumer<String> consumer) {
    for (var attr : attrs) {
      consumer.accept(attr.name);
    }
  }

  public void validate() {
    var map = new TreeMap<String, String>();
    for (var attr : attrs) {
      var old = map.put(attr.name, attr.name);
      assert old == null : "Duplicate key " + attr.name;
    }
  }

  private Set<String> keys() {
    var map = new TreeMap<String, Boolean>();
    for (var attr : attrs) {
      map.put(attr.name, Boolean.TRUE);
    }
    return map.keySet();
  }

  @Override
  public int hashCode() {
    return keys().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof AttrSet s && keys().equals(s.keys());
  }

  @Override
  public String toString() {
    return keys().toString();
  }
}
