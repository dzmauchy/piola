package org.dauch.piola.api.attributes;

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

import org.dauch.piola.attributes.AttrSet;
import org.dauch.piola.attributes.FileAttr;

import java.time.Duration;

import static org.dauch.piola.attributes.AttrCodec.*;

public interface TopicFileAttrs {

  FileAttr<Integer> PARTITIONS = new FileAttr<>("partitions", INT_CODEC, partitions -> {
    if (partitions >= 65535 || partitions < 1) {
      throw new IllegalArgumentException("Invalid partition number: " + partitions);
    }
  });

  FileAttr<Boolean> MAP = new FileAttr<>("map", BOOLEAN_CODEC, _ -> {});
  FileAttr<Duration> KEY_TTL = new FileAttr<>("key_ttl", DURATION_CODEC, _ -> {});
  FileAttr<Duration> TTL = new FileAttr<>("ttl", DURATION_CODEC, _ -> {});

  AttrSet ALL_TOPIC_ATTRS = new AttrSet(PARTITIONS, MAP, KEY_TTL, TTL);
}
