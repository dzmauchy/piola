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

import org.dauch.piola.api.request.*;
import org.dauch.piola.api.response.*;
import org.dauch.piola.attributes.EmptyAttrs;
import org.dauch.piola.attributes.SimpleAttrs;
import org.dauch.piola.client.Client;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface AnyServerTestBase {

  Server getServer();
  Client getClient();
  InetSocketAddress getAddress();

  @Test
  default void create_get_delete() throws Exception {
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicCreateRequest("t1", attrs), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicResponse("t1", attrs), rs);
    }
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicCreateRequest("t1", null), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicResponse("t1", attrs), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicGetRequest("t4"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicNotFoundResponse(), rs);
    }
    {
      // when
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", 10);
      var rs = getClient().send(new TopicGetRequest("t1"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicResponse("t1", attrs), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicCreateRequest("t2", null), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicResponse("t2", EmptyAttrs.EMPTY_ATTRS), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t2"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicResponse("t2", EmptyAttrs.EMPTY_ATTRS), rs);
    }
  }

  @Test
  default void listAll() throws Exception {
    // given
    var expected = new ArrayList<TopicResponse>();
    for (var i = 0; i < 10; i++) {
      var attrs = new SimpleAttrs();
      attrs.putInt("partitions", i + 1);
      var rq = new TopicCreateRequest("t" + i, attrs);
      var rs = getClient().send(rq, getAddress()).poll(10L, SECONDS).response();
      assertEquals(new TopicResponse("t" + i, attrs), rs);
      expected.add((TopicResponse) rs);
    }
    // when
    var list = getClient().send(new TopicListRequest("", "", ""), getAddress())
      .poll(new ArrayList<TopicResponse>(), TopicListResponse.collect(ArrayList::add))
      .get(10L, SECONDS);
    list.sort(Comparator.comparing(TopicResponse::topic));
    // then
    assertEquals(expected, list);
  }
}
