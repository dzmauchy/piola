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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Objects;
import java.util.TreeMap;

public final class Attributes {

  private final Path path;
  private final UserDefinedFileAttributeView attrs;
  private final ByteBuffer buffer;
  private final TreeMap<String, String> values = new TreeMap<>();

  public Attributes(Path path, UserDefinedFileAttributeView attrs, ByteBuffer buffer) {
    this.path = path;
    this.buffer = buffer;
    try {
      this.attrs = Objects.requireNonNull(attrs, () -> "No attributes found on " + path);
      attrs.list().forEach(name -> values.put(name, readAttr(name)));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to read attributes from " + path, e);
    }
  }

  public void write(String name, String value) {
    var encoder = StandardCharsets.UTF_8.newEncoder();
    var result = encoder.encode(CharBuffer.wrap(value), buffer.clear(), true);
    try {
      if (result == CoderResult.UNDERFLOW) {
        attrs.write(name, buffer.flip());
        values.put(name, value);
      } else {
        result.throwException();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public String get(String name) {
    return values.get(name);
  }

  private String readAttr(String name) {
    try {
      var count = attrs.read(name, buffer.clear());
      var out = CharBuffer.allocate(count);
      var decoder = StandardCharsets.UTF_8.newDecoder();
      var result = decoder.decode(buffer.flip(), out, true);
      if (result != CoderResult.UNDERFLOW) {
        result.throwException();
      }
      return out.flip().toString();
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to get attribute " + name + " from " + path, e);
    } catch (Throwable e) {
      throw new IllegalStateException("Unable to get attribute " + name + " from " + path, e);
    }
  }

  public boolean exists(String name) {
    return values.containsKey(name);
  }
}
