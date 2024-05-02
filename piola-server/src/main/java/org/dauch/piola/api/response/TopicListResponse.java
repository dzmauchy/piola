package org.dauch.piola.api.response;

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

import org.dauch.piola.client.ClientResponse;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public sealed interface TopicListResponse extends Response permits ErrorResponse, TopicDataResponse {

  static <A> BiFunction<A, ClientResponse<? extends TopicListResponse>, A> reduce(BiFunction<A, TopicDataResponse, A> f) {
    return (a, e) -> switch (e.response()) {
      case TopicDataResponse r -> r.isEndOfInput() ? null : f.apply(a, r);
      case ErrorResponse r -> throw r.toException();
    };
  }

  static <A> BiFunction<A, ClientResponse<? extends TopicListResponse>, A> collect(BiConsumer<A, TopicDataResponse> f) {
    return (a, e) -> switch (e.response()) {
      case TopicDataResponse r -> {
        if (r.isEndOfInput()) yield null;
        else {
          f.accept(a, r);
          yield a;
        }
      }
      case ErrorResponse r -> throw r.toException();
    };
  }
}
