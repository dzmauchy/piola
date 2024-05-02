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

import org.dauch.piola.api.attributes.TopicFileAttrs;
import org.dauch.piola.test.UserDirTempDirFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;

import static org.junit.jupiter.api.Assertions.*;

class AttributesTest {

  @Test
  void absentAttrs(@TempDir(factory = UserDirTempDirFactory.class) Path tempDir) throws Exception {
    // given
    var dir = tempDir.resolve("dir");
    var attrDir = Files.createDirectory(dir);
    var attrView = attrDir.getFileSystem().provider().getFileAttributeView(attrDir, UserDefinedFileAttributeView.class);
    var attrs = new Attributes(attrDir, attrView, ByteBuffer.allocateDirect(128));
    // when
    var v = attrs.get("abc");
    // then
    assertNull(v);
  }

  @Test
  void persist(@TempDir(factory = UserDirTempDirFactory.class) Path tempDir) throws Exception {
    // given
    var dir = tempDir.resolve("dir");
    var attrDir = Files.createDirectory(dir);
    var attrView = attrDir.getFileSystem().provider().getFileAttributeView(attrDir, UserDefinedFileAttributeView.class);

    {
      // given
      var attrs = new Attributes(attrDir, attrView, ByteBuffer.allocateDirect(128));
      // when
      attrs.write("abc", "cdf");
      attrs.write("xyz", "");
      // then
      assertEquals("cdf", attrs.get("abc"));
      assertEquals("", attrs.get("xyz"));
      assertNull(attrs.get("v"));
      assertTrue(attrs.exists("abc"));
      assertTrue(attrs.exists("xyz"));
      assertFalse(attrs.exists("v"));
    }

    {
      // when
      var attrs = new Attributes(attrDir, attrView, ByteBuffer.allocateDirect(128));
      attrs.write("v", "a");
      // then
      assertEquals("cdf", attrs.get("abc"));
      assertEquals("", attrs.get("xyz"));
      assertEquals("a", attrs.get("v"));
      assertTrue(attrs.exists("abc"));
      assertTrue(attrs.exists("xyz"));
      assertTrue(attrs.exists("v"));
      assertFalse(attrs.exists(" "));
    }
  }

  @Test
  void fileAttrs(@TempDir(factory = UserDirTempDirFactory.class) Path tempDir) throws Exception {
    // given
    var dir = tempDir.resolve("dir");
    var attrDir = Files.createDirectory(dir);
    var attrView = attrDir.getFileSystem().provider().getFileAttributeView(attrDir, UserDefinedFileAttributeView.class);

    // when
    {
      var attrs = new Attributes(attrDir, attrView, ByteBuffer.allocateDirect(256));
      var fa = new FileAttrs(attrs, TopicFileAttrs.ALL_TOPIC_ATTRS);
      fa.put(TopicFileAttrs.PARTITIONS, 10);
      fa.put(TopicFileAttrs.MAP, true);
      fa.writeTo(attrs);
    }

    // then
    {
      var attrs = new Attributes(attrDir, attrView, ByteBuffer.allocateDirect(256));
      var fa = new FileAttrs(attrs, TopicFileAttrs.ALL_TOPIC_ATTRS);
      assertEquals(10, fa.get(TopicFileAttrs.PARTITIONS));
      assertEquals(true, fa.get(TopicFileAttrs.MAP));
    }
  }
}
