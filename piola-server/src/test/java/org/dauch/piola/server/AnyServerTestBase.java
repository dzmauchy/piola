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
import org.dauch.piola.attributes.FileAttrs;
import org.dauch.piola.client.Client;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.*;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dauch.piola.api.attributes.TopicFileAttrs.KEY_TTL;
import static org.dauch.piola.api.attributes.TopicFileAttrs.PARTITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface AnyServerTestBase {

  Server getServer();
  Client getClient();
  InetSocketAddress getAddress();

  @Test
  default void create_get_delete() throws Exception {
    {
      // when
      var rs = getClient().send(new TopicCreateRequest("t1", new FileAttrs().put(PARTITIONS, 10)), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicDataResponse("t1", new FileAttrs().put(PARTITIONS, 10)), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicCreateRequest("t1", new FileAttrs()), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicAlreadyExistsResponse(), rs);
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
      var rs = getClient().send(new TopicGetRequest("t1"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicDataResponse("t1", new FileAttrs().put(PARTITIONS, 10)), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicCreateRequest("t2", null), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicDataResponse("t2", new FileAttrs()), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t2"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicDataResponse("t2", new FileAttrs()), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t2"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicNotFoundResponse(), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t1"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicDataResponse("t1", new FileAttrs().put(PARTITIONS, 10)), rs);
    }
    {
      // when
      var rs = getClient().send(new TopicDeleteRequest("t3"), getAddress())
        .poll(10L, SECONDS)
        .response();
      // then
      assertEquals(new TopicNotFoundResponse(), rs);
    }
    {
      // when
      var list = getClient().send(new TopicListRequest("", "", ""), getAddress())
        .poll(new ArrayList<String>(), TopicListResponse.collect((a, e) -> a.add(e.topic())))
        .get(10L, SECONDS);
      // then
      assertEquals(List.of(), list);
    }
  }

  @Test
  default void listAll() throws Exception {
    // given
    var expected = new ArrayList<TopicDataResponse>();
    for (var i = 0; i < 10; i++) {
      var attrs = new FileAttrs().put(PARTITIONS, i + 1).put(KEY_TTL, ofSeconds(i + 1));
      var rq = new TopicCreateRequest("t" + i, attrs);
      var rs = getClient().send(rq, getAddress()).poll(10L, SECONDS).response();
      assertEquals(new TopicDataResponse("t" + i, attrs), rs);
      expected.add((TopicDataResponse) rs);
    }
    // when
    var list = getClient().send(new TopicListRequest("", "", ""), getAddress())
      .poll(new ArrayList<TopicDataResponse>(), TopicListResponse.collect(ArrayList::add))
      .get(10L, SECONDS);
    list.sort(Comparator.comparing(TopicDataResponse::topic));
    // then
    assertEquals(expected, list);
  }
}
