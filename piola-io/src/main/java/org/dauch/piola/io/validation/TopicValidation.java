package org.dauch.piola.io.validation;

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

import org.dauch.piola.io.api.request.*;
import org.dauch.piola.io.exception.ValidationException;

public final class TopicValidation {

  private TopicValidation() {
  }

  public static void validate(TopicCreateRequest request) {
  }

  public static void validate(TopicDeleteRequest request) {
  }

  public static void validate(TopicGetRequest request) {
  }

  public static void validateName(String topic) {
    if (topic == null) {
      throw new NullPointerException("Topic name cannot be null");
    }
    if (topic.length() > 256) {
      throw new ValidationException("Invalid topic length: " + topic.length(), null);
    }
    if (!topic.chars().allMatch(v -> Character.isLetterOrDigit(v) || v == '_')) {
      throw new ValidationException("Invalid topic name: " + topic, null);
    }
  }

  private static void validatePartitions(int partitions) {
    if (partitions > 65536) {
      throw new ValidationException("Invalid partitions count: " + partitions, null);
    }
  }
}
