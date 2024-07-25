package org.dauch.piola.io.api.request;

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

import org.dauch.piola.io.annotation.*;
import org.dauch.piola.io.api.response.UnknownResponse;
import org.dauch.piola.io.attributes.Attrs;
import org.dauch.piola.io.attributes.EmptyAttrs;

@Serde
public record DataSendRequest(
  @Id(1) @Default("\"default\"") String topic,
  @Id(2) @Default("defaultLabels()") Attrs labels
) implements Request<UnknownResponse> {

  @Override
  public boolean hasPayload() {
    return true;
  }

  public static Attrs defaultLabels() {
    return EmptyAttrs.EMPTY_ATTRS;
  }
}
