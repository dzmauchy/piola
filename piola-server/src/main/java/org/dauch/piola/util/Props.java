package org.dauch.piola.util;

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

import java.io.UncheckedIOException;
import java.net.*;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public final class Props {

  private final Properties properties;

  public Props(Properties properties) {
    this.properties = properties;
  }

  public String get(String key, String def) {
    return properties.getProperty(key, def).trim();
  }

  public Path get(String key, Path def) {
    var v = properties.getProperty(key);
    return v == null ? def : Path.of(v);
  }

  public boolean get(String key, boolean def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Boolean.parseBoolean(sv);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public int get(String key, int def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Integer.parseInt(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public long get(String key, long def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Long.parseLong(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public double get(String key, double def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Double.parseDouble(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public float get(String key, float def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Float.parseFloat(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public InetAddress get(String key, InetAddress def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return InetAddress.ofLiteral(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public InetSocketAddress get(String key, InetSocketAddress def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return socketAddress(sv.trim());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public InetSocketAddress[] get(String key, InetSocketAddress[] def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    try {
      return Pattern.compile(",").splitAsStream(sv)
        .map(e -> socketAddress(e.trim()))
        .toArray(InetSocketAddress[]::new);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  public <E extends Enum<E>> E get(String key, E def) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def;
    }
    return Enum.valueOf(def.getDeclaringClass(), sv);
  }

  public NetworkInterface get(String key, NetworkInterface def) {
    var v = properties.getProperty(key);
    if (v == null) {
      return def;
    }
    try {
      return NetworkInterface.getByName(v);
    } catch (SocketException e) {
      throw new UncheckedIOException(e);
    }
  }

  public <T> T get(String key, Supplier<? extends T> def, Function<String, ? extends T> func) {
    var sv = properties.getProperty(key);
    if (sv == null) {
      return def.get();
    }
    try {
      return func.apply(sv);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + "(" + sv + ")", e);
    }
  }

  private static InetSocketAddress socketAddress(String v) {
    var pos = v.lastIndexOf(':');
    if (pos < 0) {
      throw new NoSuchElementException(":");
    }
    var addr = InetAddress.ofLiteral(v.substring(0, pos));
    var port = Integer.parseInt(v.substring(pos + 1));
    return new InetSocketAddress(addr, port);
  }
}
