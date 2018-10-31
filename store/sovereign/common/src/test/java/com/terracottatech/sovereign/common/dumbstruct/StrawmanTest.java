/*
 * Copyright (c) 2012-2018 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.terracottatech.sovereign.common.dumbstruct;

import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int8Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.StringField;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

public class StrawmanTest {

  static final class SinglyLinkedNode {

    private static final Int8Field slen;

    private static final StringField payload;

    private static final Int32Field next;

    private static final Struct struct;

    static {
      StructBuilder b = new StructBuilder();
      slen = b.int8();
      payload = b.string(40);
      next = b.int32();
      struct = b.end();
    }

    public SinglyLinkedNode() {
    }

    public void init(Accessor a, String value) {
      setPayload(a, value);
      next.put(a, -1);
    }

    public void setPayload(Accessor a, String s) {
      slen.put(a, (byte) s.length());
      payload.putString(a, s);
    }

    public String getPayload(Accessor a) {
      int i = slen.get(a);
      return payload.getString(a, i);
    }

    public void setNext(Accessor a, int p) {
      next.put(a, p);
    }

    public int getNext(Accessor a) {
      return next.get(a);
    }

    public StructField getStruct() {
      return struct;
    }
  }

  @Test
  public void testSinglyLinkedList() {
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(10 * 1024));
    int last = 0;

    Accessor a = new Accessor(db, last);
    SinglyLinkedNode node1 = new SinglyLinkedNode();
    node1.init(a, "0!");

    for (int i = 1; i < 10; i++) {
      a=a.increment(node1.getStruct()); // simple allocation
      SinglyLinkedNode nextNode = new SinglyLinkedNode();
      nextNode.init(a, i + "!");
      nextNode.setNext(a, last);
      last = a.getOffset();
    }

    SinglyLinkedNode n = new SinglyLinkedNode();
    int expected = 9;
    while (a.getOffset() >= 0) {
      Assert.assertThat(n.getPayload(a), is(expected + "!"));
      a=a.repoint(n.getNext(a));
      expected--;
    }

  }

  static final class DoublyLinkedNode {

    private static final Int8Field slen;

    private static final StringField payload;

    private static final Int32Field next;

    private static final Int32Field prev;

    private static final Struct struct;

    static {
      StructBuilder b = new StructBuilder();
      slen = b.int8();
      payload = b.string(40);
      next = b.int32();
      prev = b.int32();
      struct = b.end();
    }

    public DoublyLinkedNode() {
    }

    public void init(Accessor a, String value) {
      setPayload(a, value);
      next.put(a, -1);
      prev.put(a, -1);
    }

    public void setPayload(Accessor a, String s) {
      slen.put(a, (byte) s.length());
      payload.putString(a, s);
    }

    public String getPayload(Accessor a) {
      int i = slen.get(a);
      return payload.getString(a, i);
    }

    public void setNext(Accessor a, int p) {
      next.put(a, p);
    }

    public int getNext(Accessor a) {
      return next.get(a);
    }

    public void setPrev(Accessor a, int p) {
      prev.put(a, p);
    }

    public int getPrev(Accessor a) {
      return prev.get(a);
    }

    public StructField getStruct() {
      return struct;
    }
  }

  @Test
  public void testDoublyLinkedList() {
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(10 * 1024));

    Accessor a = new Accessor(db, 0);
    DoublyLinkedNode n = new DoublyLinkedNode();
    n.init(a, "0--");

    for (int i = 1; i < 10; i++) {
      Accessor lastAddr = a;
      a = a.increment(n.getStruct()); // simple allocation
      n.init(a, i + "--");
      n.setPrev(a, lastAddr.getOffset());
      n.setNext(lastAddr, a.getOffset());
    }

    a=a.repoint(0);
    int expected = 0;
    while (a.getOffset() >= 0) {
      Assert.assertThat(n.getPayload(a), is(expected++ + "--"));
      a=a.repoint(n.getNext(a));
    }
  }

  static class Stack1 {
    private static final Int32Field slen;

    private static final Int32Field many;

    private static final StringField payload;

    private static final Struct stackstruct;

    private static final Struct struct;

    static {
      StructBuilder b = new StructBuilder();
      many = b.int32();
      b.substruct();
      slen = b.int32();
      payload = b.string(20);
      stackstruct = b.end(10);
      struct = b.end(1);
    }

    public Stack1() {
    }

    public void initEmpty(Accessor a) {
      many.put(a, 0);
    }

    public String pop(Accessor a) {
      if (many.get(a) <= 0) {
        throw new IndexOutOfBoundsException();
      }
      int m = many.get(a);
      many.put(a, --m);
      // soup two ways
      Accessor tmp = a.increment(stackstruct.deltaFor(m));
      int l = slen.get(tmp);
      return payload.getString(tmp, l);
    }

    public void push(Accessor a, String s) {
      if (many.get(a) >= stackstruct.getAllocationCount()) {
        throw new IndexOutOfBoundsException();
      }
      int m = many.get(a);
      // soup two ways
      Accessor tmp = a.increment(stackstruct, m);
      slen.put(tmp, s.length());
      payload.putString(tmp, s);
      many.put(a, ++m);
    }

    public String peek(Accessor a) {
      if (many.get(a) <= 0) {
        throw new IndexOutOfBoundsException();
      }
      int m = many.get(a) - 1;
      // soup two ways
      Accessor tmp = a.increment(stackstruct.deltaFor(m));
      int l = slen.get(tmp);
      return payload.getString(tmp, l);
    }

    public int size(Accessor a) {
      return many.get(a);
    }
  }

  @Test
  public void testStack1() {
    SingleDataByteBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(10 * 1024));

    Accessor a = new Accessor(db, 0);
    Stack1 stack = new Stack1();
    stack.initEmpty(a);
    Assert.assertThat(stack.size(a), is(0));

    stack.push(a, "one");
    Assert.assertThat(stack.peek(a), is("one"));
    stack.push(a, "two");
    Assert.assertThat(stack.peek(a), is("two"));
    stack.push(a, "three");
    Assert.assertThat(stack.peek(a), is("three"));
    Assert.assertThat(stack.size(a), is(3));

    Assert.assertThat(stack.pop(a), is("three"));
    Assert.assertThat(stack.pop(a), is("two"));
    Assert.assertThat(stack.pop(a), is("one"));
    Assert.assertThat(stack.size(a), is(0));

  }

}
