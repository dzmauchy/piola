package org.dauch.piola.io.api;

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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.stream.*;

import static java.util.Collections.binarySearch;
import static java.util.Comparator.comparingInt;

public final class SerializationContext {

  private final int id;
  private final BitSet unknownFields;
  private final ArrayList<SerializationContext> children = new ArrayList<>(0);

  public SerializationContext() {
    this(0);
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

  private static SerializationContext read(int id, ByteBuffer buffer) {
    var len = Byte.toUnsignedInt(buffer.get());
    var set = BitSet.valueOf(buffer.slice(buffer.position(), len));
    var ctx = new SerializationContext(id, set);
    buffer.position(buffer.position() + len);
    while ((id = Byte.toUnsignedInt(buffer.get())) != 0) {
      ctx.children.add(read(id, buffer));
    }
    return ctx;
  }

  public static SerializationContext read(ByteBuffer buffer) {
    return read(Byte.toUnsignedInt(buffer.get()), buffer);
  }

  public boolean isEmpty() {
    return unknownFields.isEmpty() && children.stream().allMatch(SerializationContext::isEmpty);
  }

  public int unknownFieldsCount() {
    return unknownFields.size();
  }

  public void write(ByteBuffer buffer) {
    var data = unknownFields.toByteArray();
    buffer.put((byte) id).put((byte) data.length).put(data);
    children.forEach(v -> {
      if (!v.isEmpty()) {
        v.write(buffer);
      }
    });
    buffer.put((byte) 0);
  }

  @Override
  public int hashCode() {
    return children()
      .mapToInt(SerializationContext::hashCode)
      .reduce(unknownFields.hashCode() ^ id, (a, b) -> 31 * a + b);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SerializationContext that)) {
      return false;
    }
    if (!unknownFields.equals(that.unknownFields)) {
      return false;
    }
    for (var thisChild : this.children) {
      var i = binarySearch(that.children, thisChild, comparingInt(SerializationContext::id));
      if (i < 0 && !thisChild.isEmpty() || i >= 0 && !that.children.get(i).equals(thisChild))
        return false;
    }
    for (var thatChild : that.children) {
      var i = binarySearch(this.children, thatChild, comparingInt(SerializationContext::id));
      if (i < 0 && !thatChild.isEmpty() || i >= 0 && !this.children.get(i).equals(thatChild)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "Ctx(%d,%s,%s)".formatted(
      id,
      unknownFields,
      children()
        .map(SerializationContext::toString)
        .collect(Collectors.joining(",", "[", "]"))
    );
  }
}
