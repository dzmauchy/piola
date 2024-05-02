package org.dauch.piola.buffer;

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

public record CloseableBuffer(ByteBuffer buffer, AutoCloseable cleaner) implements AutoCloseable {

  @Override
  public void close() throws Exception {
    if (cleaner != null) {
      cleaner.close();
    }
  }

  public static void write(Iterable<CloseableBuffer> buffers, ByteBuffer target) throws Exception {
    var exception = (Throwable) null;
    for (var buf : buffers) {
      try (buf) {
        target.put(buf.buffer());
      } catch (Throwable e) {
        if (exception == null) exception = e;
        else exception.addSuppressed(e);
      }
    }
    switch (exception) {
      case null -> {}
      case Error e -> throw e;
      case RuntimeException e -> throw e;
      case Exception e -> throw e;
      default -> throw new UnknownError("Invalid type: " + exception.getClass());
    }
  }
}
