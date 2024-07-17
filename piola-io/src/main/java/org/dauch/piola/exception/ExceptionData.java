package org.dauch.piola.exception;

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

import org.dauch.piola.api.Serialization;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public record ExceptionData(
  int id,
  String type,
  String message,
  ExceptionData cause,
  StackTraceLine[] stackTrace,
  ExceptionData[] suppressed
) {

  public static ExceptionData from(Throwable e) {
    return from(e, new IdentityHashMap<>(1), new AtomicInteger());
  }

  private static ExceptionData from(Throwable throwable, IdentityHashMap<Throwable, Integer> passed, AtomicInteger c) {
    if (throwable == null) {
      return null;
    }
    var type = throwable.getClass().getName();
    var msg = Objects.requireNonNull(throwable.getMessage(), "");
    var id = c.getAndIncrement();
    var oldId = passed.put(throwable, id);
    if (oldId == null) {
      var cause = from(throwable.getCause(), passed, c);
      var suppressed = Arrays.stream(throwable.getSuppressed())
        .map(e -> from(e, passed, c))
        .filter(Objects::nonNull)
        .toArray(ExceptionData[]::new);
      var stacktrace = Arrays.stream(throwable.getStackTrace())
        .filter(e -> e.getClassName().startsWith("org.dauch."))
        .map(e -> new StackTraceLine(e.getClassName(), e.getMethodName(), e.getLineNumber()))
        .toArray(StackTraceLine[]::new);
      return new ExceptionData(id, type, msg, cause, stacktrace, suppressed);
    } else {
      return new ExceptionData(oldId, "", "", null, new StackTraceLine[0], new ExceptionData[0]);
    }
  }

  public static ExceptionData read(ByteBuffer buffer) {
    var id = buffer.getInt();
    var type = Serialization.read(buffer, (String) null);
    var message = Serialization.read(buffer, (String) null);
    var cause = Serialization.read(buffer, (ExceptionData) null);
    var stc = new StackTraceLine[buffer.getInt()];
    for (int i = 0; i < stc.length; i++) {
      stc[i] = StackTraceLine.read(buffer);
    }
    var sup = new ExceptionData[buffer.getInt()];
    for (int i = 0; i < sup.length; i++) {
      sup[i] = ExceptionData.read(buffer);
    }
    return new ExceptionData(id, type, message, cause, stc, sup);
  }

  public void write(ByteBuffer buffer) {
    buffer.putInt(id);
    Serialization.write(buffer, type);
    Serialization.write(buffer, message);
    Serialization.write(buffer, cause);
    buffer.putInt(stackTrace.length);
    for (var l : stackTrace) {
      l.write(buffer);
    }
    buffer.putInt(suppressed.length);
    for (var s : suppressed) {
      s.write(buffer);
    }
  }

  @Override
  public String toString() {
    return "Exception(%d,%s,%s,%s,%s,%s)".formatted(
      id,
      type,
      message,
      cause,
      Arrays.toString(stackTrace),
      Arrays.toString(suppressed)
    );
  }
}
