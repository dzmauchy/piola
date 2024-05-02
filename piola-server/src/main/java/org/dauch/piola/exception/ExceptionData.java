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

import org.dauch.piola.util.Serialization;

import java.nio.ByteBuffer;
import java.util.*;

public record ExceptionData(String type, String message, ExceptionData cause, StackTraceLine[] stackTrace, ExceptionData[] suppressed) {

  public static ExceptionData from(Throwable e) {
    return from(e, new IdentityHashMap<>(1));
  }

  private static ExceptionData from(Throwable throwable, IdentityHashMap<Throwable, Boolean> passed) {
    if (throwable == null) {
      return null;
    }
    if (passed.put(throwable, Boolean.TRUE) == null) {
      var type = throwable.getClass().getName();
      var msg = Objects.requireNonNull(throwable.getMessage(), "");
      var cause = from(throwable.getCause(), passed);
      var suppressed = Arrays.stream(throwable.getSuppressed())
        .map(e -> from(e, passed))
        .filter(Objects::nonNull)
        .toArray(ExceptionData[]::new);
      var stacktrace = Arrays.stream(throwable.getStackTrace())
        .filter(e -> e.getClassName().startsWith("org.dauch."))
        .map(e -> new StackTraceLine(e.getClassName(), e.getMethodName(), e.getLineNumber()))
        .toArray(StackTraceLine[]::new);
      return new ExceptionData(type, msg, cause, stacktrace, suppressed);
    } else {
      return null;
    }
  }

  public static ExceptionData read(ByteBuffer buffer) {
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
    return new ExceptionData(type, message, cause, stc, sup);
  }

  public void write(ByteBuffer buffer) {
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
}
