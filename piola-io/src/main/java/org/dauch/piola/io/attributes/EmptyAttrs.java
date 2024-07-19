package org.dauch.piola.io.attributes;

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

import org.dauch.piola.io.exception.NoValueException;

import java.nio.ByteBuffer;

public final class EmptyAttrs extends Attrs {

  public static final EmptyAttrs EMPTY_ATTRS = new EmptyAttrs();

  private EmptyAttrs() {
  }

  @Override
  long readRaw(long key) throws NoValueException {
    throw NoValueException.NO_VALUE_EXCEPTION;
  }

  @Override
  int size() {
    return 0;
  }

  @Override
  long getKeyByIndex(int index) {
    throw new IndexOutOfBoundsException();
  }

  @Override
  long getValueByIndex(int index) {
    throw new IndexOutOfBoundsException();
  }

  @Override
  public void write(ByteBuffer buffer) {
    buffer.putInt(0);
  }
}
