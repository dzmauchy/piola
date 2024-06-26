package org.dauch.piola.api.request;

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

import org.dauch.piola.annotation.*;
import org.dauch.piola.api.response.TopicCreateResponse;
import org.dauch.piola.attributes.Attrs;
import org.dauch.piola.attributes.EmptyAttrs;

@Serde
public record TopicCreateRequest(
  @Id(0x01) @Default("\"default\"") String topic,
  @Id(0x02) @Default("emptyAttrs()") Attrs attrs
) implements Request<TopicCreateResponse> {

  public static EmptyAttrs emptyAttrs() {
    return EmptyAttrs.EMPTY_ATTRS;
  }
}
