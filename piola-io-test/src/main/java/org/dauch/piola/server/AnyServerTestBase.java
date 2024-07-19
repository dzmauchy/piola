package org.dauch.piola.server;

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
import org.dauch.piola.io.api.response.TopicInfoResponse;
import org.dauch.piola.io.api.response.TopicNotFoundResponse;
import org.dauch.piola.io.attributes.EmptyAttrs;
import org.dauch.piola.io.attributes.SimpleAttrs;
import org.dauch.piola.io.client.Client;
import org.dauch.piola.io.server.Server;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.waitAtMost;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface AnyServerTestBase {

  Server getServer();
  Client getClient();
  InetSocketAddress getAddress();

  @Test
  default void create_get_delete() {
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicCreateRequest("t1", attrs), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicInfoResponse("t1", attrs), rs);
    }
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicCreateRequest("t1", null), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicInfoResponse("t1", attrs), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicGetRequest("t4"), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicNotFoundResponse(), rs);
    }
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicGetRequest("t1"), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicInfoResponse("t1", attrs), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicCreateRequest("t2", null), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicInfoResponse("t2", EmptyAttrs.EMPTY_ATTRS), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t2"), null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      // then
      assertEquals(new TopicInfoResponse("t2", EmptyAttrs.EMPTY_ATTRS), rs);
    }
  }

  @Test
  default void listAll() {
    // given
    var expected = new HashSet<TopicInfoResponse>();
    for (var i = 0; i < 10; i++) {
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", i + 1);
      var rq = new TopicCreateRequest("t" + i, attrs);
      var rs = getClient()
        .send(rq, null, 0, getAddress())
        .poll(3L, SECONDS)
        .response();
      assertEquals(new TopicInfoResponse("t" + i, attrs), rs);
      expected.add((TopicInfoResponse) rs);
    }
    // when
    var resp = getClient().send(new TopicListRequest("", "", ""), null, 0, getAddress());
    // then
    var list = new HashSet<TopicInfoResponse>();
    var end = new AtomicBoolean();
    waitAtMost(10L, SECONDS).until(() -> resp.poll(r -> {
      if (r.response() instanceof TopicInfoResponse tir) {
        if (tir.isEndOfInput()) {
          end.set(true);
        } else {
          list.add(tir);
        }
      }
    }) && end.get());
    assertEquals(expected, list);
  }

  @Test
  default void send_many_attributes() {
    // given
    var attrs = new SimpleAttrs();
    for (int i = 0; i < 10000; i++) {
      attrs.putLong("a" + i, i);
    }
    // when
    var rs = getClient().send(new TopicCreateRequest("t1", attrs), null, 0, getAddress())
      .poll(3L, SECONDS)
      .response();
    // then
    if (rs instanceof TopicInfoResponse r) {
      assertEquals(attrs, r.attrs());
    } else {
      throw new AssertionError("Expected TopicInfoResponse, got " + rs);
    }
  }
}
