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

import org.dauch.piola.api.response.*;
import org.dauch.piola.api.serde.*;

import java.nio.ByteBuffer;

public final class ResponseFactory {

  private ResponseFactory() {}

  public static Response read(ByteBuffer input, SerializationContext context) {
    var code = input.getInt();
    return switch (code) {
      // common codes
      case 1 -> ErrorResponseSerde.read(input, context);
      case 2 -> UnknownResponseSerde.read(input, context);
      // topics
      case 1000 -> TopicResponseSerde.read(input, context);
      case 1001 -> TopicNotFoundResponseSerde.read(input, context);
      // otherwise
      default -> new UnknownResponse(code);
    };
  }

  public static void write(Response response, ByteBuffer output) {
    switch (response) {
      // common codes
      case ErrorResponse r -> ErrorResponseSerde.write(r, output.putInt(1));
      case UnknownResponse r -> UnknownResponseSerde.write(r, output.putInt(2));
      // topics
      case TopicResponse r -> TopicResponseSerde.write(r, output.putInt(1000));
      case TopicNotFoundResponse r -> TopicNotFoundResponseSerde.write(r, output.putInt(1001));
    }
  }
}
