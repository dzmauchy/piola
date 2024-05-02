package org.dauch.piola.api;

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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.binarySearch;
import static java.util.Comparator.comparingInt;

public final class SerializationContext {

  private final int id;
  private final BitSet unknownFields;
  private final ArrayList<SerializationContext> children = new ArrayList<>(0);

  public SerializationContext() {
    this(-1);
  }

  private SerializationContext(int id) {
    this(id, new BitSet());
  }

  private SerializationContext(int id, BitSet unknownFields) {
    this.id = id;
    this.unknownFields = unknownFields;
  }

  public int id() {
    return id;
  }

  public void addUnknownField(int field) {
    unknownFields.set(field);
  }

  public IntStream unknownFields() {
    return unknownFields.stream();
  }

  public Stream<SerializationContext> children() {
    return children.stream().filter(e -> !e.isEmpty());
  }

  public SerializationContext child(int id) {
    var toFind = new SerializationContext(id);
    var index = binarySearch(children, toFind, comparingInt(SerializationContext::id));
    if (index < 0) {
      children.add(-(index + 1), toFind);
      return toFind;
    } else {
      return children.get(index);
    }
  }

  public static SerializationContext read(ByteBuffer buffer) {
    var id = Byte.toUnsignedInt(buffer.get());
    var set = BitSet.valueOf(buffer.slice(buffer.position(), Byte.toUnsignedInt(buffer.get())));
    var ctx = new SerializationContext(id, set);
    for (int i = 0, childrenCount = Byte.toUnsignedInt(buffer.get()); i < childrenCount; i++) {
      ctx.children.add(SerializationContext.read(buffer));
    }
    return ctx;
  }

  public boolean isEmpty() {
    return unknownFields.isEmpty() && children.isEmpty();
  }

  public int unknownFieldsCount() {
    return unknownFields.size();
  }

  public int childrenCount() {
    return children.size();
  }

  public void normalize() {
    children.removeIf(v -> {
      v.normalize();
      return v.isEmpty();
    });
  }

  public void write(ByteBuffer buffer) {
    var data = unknownFields.toByteArray();
    buffer.put((byte) id).put((byte) data.length).put(data).put((byte) children.size());
    children.forEach(v -> {
      buffer.put((byte) v.id);
      v.write(buffer);
    });
  }

  @Override
  public int hashCode() {
    return unknownFields.hashCode() ^ children.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SerializationContext that) {
      return unknownFields.equals(that.unknownFields) && children.equals(that.children);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "Ctx(%d,%s,%s)".formatted(id, unknownFields, children);
  }
}
