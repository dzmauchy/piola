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

import org.dauch.piola.api.request.*;
import org.dauch.piola.api.serde.*;

import java.nio.ByteBuffer;

public final class RequestFactory {

  private RequestFactory() {
  }

  public static Request<?> request(ByteBuffer input, SerializationContext context) {
    var req = input.getInt();
    return switch (req) {
      case 1 -> TopicCreateRequestSerde.read(input, context);
      case 2 -> TopicDeleteRequestSerde.read(input, context);
      case 3 -> TopicGetRequestSerde.read(input, context);
      case 4 -> TopicListRequestSerde.read(input, context);
      case 5 -> SendDataRequestSerde.read(input, context);
      default -> new UnknownRequest(req);
    };
  }

  public static void write(Request<?> request, ByteBuffer output) {
    switch (request) {
      case TopicCreateRequest r -> TopicCreateRequestSerde.write(r, output.putInt(1));
      case TopicDeleteRequest r -> TopicDeleteRequestSerde.write(r, output.putInt(2));
      case TopicGetRequest r -> TopicGetRequestSerde.write(r, output.putInt(3));
      case TopicListRequest r -> TopicListRequestSerde.write(r, output.putInt(4));
      case SendDataRequest r -> SendDataRequestSerde.write(r, output.putInt(5));
      case UnknownRequest r -> UnknownRequestSerde.write(r, output.putInt(0));
    }
  }
}
