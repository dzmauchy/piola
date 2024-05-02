package org.dauch.piola.attributes;

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

import java.time.*;
import java.util.function.Function;

public record AttrCodec<V>(Function<? super V, String> co, Function<String, ? extends V> dec) {

  public static final AttrCodec<String> STRING_CODEC = new AttrCodec<>(Function.identity(), Function.identity());
  public static final AttrCodec<Integer> INT_CODEC = new AttrCodec<>(Object::toString, Integer::valueOf);
  public static final AttrCodec<Long> LONG_CODEC = new AttrCodec<>(Object::toString, Long::valueOf);
  public static final AttrCodec<Short> SHORT_CODEC = new AttrCodec<>(Object::toString, Short::valueOf);
  public static final AttrCodec<Byte> BYTE_CODEC = new AttrCodec<>(Object::toString, Byte::valueOf);
  public static final AttrCodec<Float> FLOAT_CODEC = new AttrCodec<>(Object::toString, Float::valueOf);
  public static final AttrCodec<Double> DOUBLE_CODEC = new AttrCodec<>(Object::toString, Double::valueOf);
  public static final AttrCodec<Boolean> BOOLEAN_CODEC = new AttrCodec<>(Object::toString, Boolean::valueOf);
  public static final AttrCodec<LocalDateTime> LOCAL_DATE_TIME_CODEC = new AttrCodec<>(LocalDateTime::toString, LocalDateTime::parse);
  public static final AttrCodec<LocalDate> LOCAL_DATE_CODEC = new AttrCodec<>(LocalDate::toString, LocalDate::parse);
  public static final AttrCodec<LocalTime> LOCAL_TIME_CODEC = new AttrCodec<>(LocalTime::toString, LocalTime::parse);
  public static final AttrCodec<Duration> DURATION_CODEC = new AttrCodec<>(Duration::toString, Duration::parse);

  public String encode(V value) {
    return co.apply(value);
  }

  public V decode(String value) {
    return dec.apply(value);
  }
}
