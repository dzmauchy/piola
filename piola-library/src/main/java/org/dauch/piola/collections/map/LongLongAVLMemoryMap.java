package org.dauch.piola.collections.map;

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

import java.util.function.LongConsumer;

public final class LongLongAVLMemoryMap {

  Node root;

  public void get(long key, LongConsumer consumer) {
    var node = get(root, key);
    if (node != null) {
      for (var item = node.value; item != null; item = item.prev) {
        consumer.accept(item.value);
      }
    }
  }

  private Node get(Node node, long key) {
    if (node == null) {
      return null;
    } else if (key < node.key) {
      return get(node.left, key);
    } else if (key > node.key) {
      return get(node.right, key);
    } else {
      return node;
    }
  }

  public void put(long key, long value) {
    root = put(root, key, value);
  }

  private Node put(Node node, long key, long value) {
    if (node == null) {
      return new Node(key, value);
    } else if (key < node.key) {
      node.left = put(node.left, key, value);
    } else if (key > node.key) {
      node.right = put(node.right, key, value);
    } else {
      node.add(value);
    }
    return balance(node);
  }

  private Node balance(Node node) {
    updateHeight(node);
    int bf = balanceFactor(node);
    if (bf > 1) {
      if (balanceFactor(node.left) < 0) {
        node.left = rotateLeft(node.left);
      }
      return rotateRight(node);
    } else if (bf < -1) {
      if (balanceFactor(node.right) > 0) {
        node.right = rotateRight(node.right);
      }
      return rotateLeft(node);
    }
    return node;
  }

  private Node rotateRight(Node n) {
    var r = n.left;
    n.left = r.right;
    r.right = n;
    updateHeight(n);
    updateHeight(r);
    return r;
  }

  private Node rotateLeft(Node n) {
    var r = n.right;
    n.right = r.left;
    r.left = n;
    updateHeight(n);
    updateHeight(r);
    return r;
  }

  private void updateHeight(Node node) {
    node.height = 1 + Math.max(height(node.left), height(node.right));
  }

  private int height(Node node) {
    return node == null ? 0 : node.height;
  }

  private int balanceFactor(Node node) {
    return node == null ? 0 : height(node.left) - height(node.right);
  }

  static final class Node {

    final long key;
    Node left, right;
    int height = 1;
    Item value;

    Node(long key, long value) {
      this.key = key;
      this.value = new Item(value, null);
    }

    void add(long value) {
      this.value = new Item(value, this.value);
    }
  }

  record Item(long value, Item prev) {}
}
