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

import org.dauch.piola.util.Serialization;

import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.function.Supplier;

public final class FileAttrs {

  private final TreeMap<String, String> values = new TreeMap<>();

  public FileAttrs() {
  }

  public FileAttrs(Attributes attributes, AttrSet attrs) {
    attrs.forEachKey(k -> {
      var v = attributes.get(k);
      if (v != null) {
        values.put(k, v);
      }
    });
  }

  public <V> FileAttrs put(FileAttr<V> attr, V value) {
    attr.validator.accept(value);
    values.put(attr.name, attr.codec.encode(value));
    return this;
  }

  public <V> V get(FileAttr<V> attr) {
    var value = values.get(attr.name);
    if (value == null) return null;
    var decoded = attr.codec.decode(value);
    attr.validator.accept(decoded);
    return decoded;
  }

  public <V> V get(FileAttr<V> attr, Supplier<V> defaultValue) {
    var v = get(attr);
    return v == null ? defaultValue.get() : v;
  }

  public static FileAttrs readFrom(ByteBuffer buffer) {
    var attrs = new FileAttrs();
    while (buffer.get() != 0) {
      attrs.values.put(Serialization.read(buffer, (String) null), Serialization.read(buffer, (String) null));
    }
    return attrs;
  }

  public void writeTo(ByteBuffer buffer) {
    values.forEach((k, v) -> {
      buffer.put((byte) 1);
      Serialization.write(buffer, k);
      Serialization.write(buffer, v);
    });
    buffer.put((byte) 0);
  }

  public void writeTo(Attributes attributes) {
    values.forEach(attributes::write);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof FileAttrs a && values.equals(a.values);
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
