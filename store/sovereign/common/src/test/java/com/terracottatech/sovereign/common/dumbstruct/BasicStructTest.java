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

import com.terracottatech.sovereign.common.dumbstruct.buffers.DataBuffer;
import com.terracottatech.sovereign.common.dumbstruct.buffers.SingleDataByteBuffer;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.ByteArrayField;
import com.terracottatech.sovereign.common.dumbstruct.fields.Char16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Float64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int16Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int32Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int64Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.Int8Field;
import com.terracottatech.sovereign.common.dumbstruct.fields.composite.StringField;
import com.terracottatech.sovereign.common.dumbstruct.fields.StructField;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static org.hamcrest.Matchers.is;

public class BasicStructTest {

  @Test
  public void testCreate() {
    LinkedList<StructField> ll = new LinkedList<StructField>();
    Int8Field f1 = new Int8Field(0, 1);
    Int32Field f2 = new Int32Field(f1.getOffsetWithinStruct() + f1.getAllocatedSize(), 1);
    Char16Field f3 = new Char16Field(f2.getOffsetWithinStruct() + f2.getAllocatedSize(), 1);
    Int8Field f4 = new Int8Field(f3.getOffsetWithinStruct() + f3.getAllocatedSize(), 1);
    ll.add(f1);
    ll.add(f2);
    ll.add(f3);
    ll.add(f4);
    BasicStruct s1 = new BasicStruct(0, 1, ll);
    Assert.assertThat(s1.getSingleFieldSize(), is(8));
    Assert.assertThat(s1.getAllocatedSize(), is(8));
    Assert.assertThat(s1.getAllocationCount(), is(1));
    Assert.assertThat(s1.getFields().size(), is(4));
  }

  @Test
  public void testAccessFieldsSingleStruct() {
    LinkedList<StructField> ll = new LinkedList<StructField>();
    Int8Field f1 = new Int8Field(0, 1);
    Int32Field f2 = new Int32Field(f1.getOffsetWithinStruct() + f1.getAllocatedSize(), 1);
    Char16Field f3 = new Char16Field(f2.getOffsetWithinStruct() + f2.getAllocatedSize(), 1);
    Int8Field f4 = new Int8Field(f3.getOffsetWithinStruct() + f3.getAllocatedSize(), 1);
    ll.add(f1);
    ll.add(f2);
    ll.add(f3);
    ll.add(f4);
    BasicStruct s1 = new BasicStruct(0, 1, ll);

    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db);
    f1.put(a, (byte) 1);
    f2.put(a, 222);
    f3.put(a, 'g');
    f4.put(a, (byte) 5);
    Assert.assertThat(f1.get(a), is((byte) 1));
    Assert.assertThat(f2.get(a), is(222));
    Assert.assertThat(f3.get(a), is('g'));
    Assert.assertThat(f4.get(a), is((byte) 5));
  }

  @Test
  public void testAccessFieldsSingleStructMovingAccessor() {
    LinkedList<StructField> ll = new LinkedList<StructField>();
    Int8Field f1 = new Int8Field(0, 1);
    Int32Field f2 = new Int32Field(f1.getOffsetWithinStruct() + f1.getAllocatedSize(), 1);
    Char16Field f3 = new Char16Field(f2.getOffsetWithinStruct() + f2.getAllocatedSize(), 1);
    Int8Field f4 = new Int8Field(f3.getOffsetWithinStruct() + f3.getAllocatedSize(), 1);
    ll.add(f1);
    ll.add(f2);
    ll.add(f3);
    ll.add(f4);
    BasicStruct s1 = new BasicStruct(0, 1, ll);

    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db);
    f1.put(a, (byte) 1);
    f2.put(a, 222);
    f3.put(a, 'g');
    f4.put(a, (byte) 5);

    a=a.increment(s1);
    f1.put(a, (byte) 2);
    f2.put(a, 223);
    f3.put(a, 'h');
    f4.put(a, (byte) 6);
    Assert.assertThat(f1.get(a), is((byte) 2));
    Assert.assertThat(f2.get(a), is(223));
    Assert.assertThat(f3.get(a), is('h'));
    Assert.assertThat(f4.get(a), is((byte) 6));

    a=a.decrement(s1);
    Assert.assertThat(f1.get(a), is((byte) 1));
    Assert.assertThat(f2.get(a), is(222));
    Assert.assertThat(f3.get(a), is('g'));
    Assert.assertThat(f4.get(a), is((byte) 5));
  }

  @Test
  public void testEveryFieldTypeMoving() {
    LinkedList<StructField> ll = new LinkedList<StructField>();
    Int8Field f1 = new Int8Field(0, 1);
    Int16Field f2 = new Int16Field(f1.getOffsetWithinStruct() + f1.getAllocatedSize(), 1);
    Int32Field f3 = new Int32Field(f2.getOffsetWithinStruct() + f2.getAllocatedSize(), 1);
    Int64Field f4 = new Int64Field(f3.getOffsetWithinStruct() + f3.getAllocatedSize(), 1);
    Char16Field f5 = new Char16Field(f4.getOffsetWithinStruct() + f4.getAllocatedSize(), 1);
    Float32Field f6 = new Float32Field(f5.getOffsetWithinStruct() + f5.getAllocatedSize(), 1);
    Float64Field f7 = new Float64Field(f6.getOffsetWithinStruct() + f6.getAllocatedSize(), 1);
    StringField f8 = new StringField(f7.getOffsetWithinStruct() + f7.getAllocatedSize(), 20);
    ByteArrayField f9 = new ByteArrayField(f8.getOffsetWithinStruct() + f8.getAllocatedSize(), 10);
    ll.addAll(Arrays.asList(new StructField[]{f1, f2, f3, f4, f5, f6, f7, f8, f9}));
    BasicStruct s1 = new BasicStruct(0, 3, ll);

    DataBuffer db = new SingleDataByteBuffer(ByteBuffer.allocate(1024));
    Accessor a = new Accessor(db);

    f1.put(a, (byte) 1);
    f2.put(a, (short) 1);
    f3.put(a, 1);
    f4.put(a, 1l);
    f5.put(a, 'a');
    f6.put(a, 1.0f);
    f7.put(a, 1.0d);
    f8.putString(a, "one");
    f9.put(a, 0, new byte[]{ 1, 1, 1}, 0, 3);

    // increment to write it...
    a=a.increment(s1);

    f1.put(a, (byte) 2);
    f2.put(a, (short) 2);
    f3.put(a, 2);
    f4.put(a, 2l);
    f5.put(a, 'b');
    f6.put(a, 2.0f);
    f7.put(a, 2.0d);
    f8.putString(a, "two");
    f9.put(a, 0, new byte[]{2, 2, 2}, 0, 3);

    // jump back
    a=a.decrement(s1);
    Assert.assertThat(f1.get(a),is((byte)1));
    Assert.assertThat(f2.get(a),is((short)1));
    Assert.assertThat(f3.get(a),is(1));
    Assert.assertThat(f4.get(a),is((long)1));
    Assert.assertThat(f5.get(a),is('a'));
    Assert.assertThat(f6.get(a),is(1.0f));
    Assert.assertThat(f7.get(a),is(1.0d));
    Assert.assertThat(f8.getString(a,3),is("one"));
    Assert.assertThat(f9.get(a,0),is((byte)1));
    Assert.assertThat(f9.get(a,1),is((byte)1));
    Assert.assertThat(f9.get(a,2),is((byte)1));

    a=a.increment(s1);
    Assert.assertThat(f1.get(a),is((byte)2));
    Assert.assertThat(f2.get(a),is((short)2));
    Assert.assertThat(f3.get(a),is(2));
    Assert.assertThat(f4.get(a),is((long)2));
    Assert.assertThat(f5.get(a),is('b'));
    Assert.assertThat(f6.get(a),is(2.0f));
    Assert.assertThat(f7.get(a),is(2.0d));
    Assert.assertThat(f8.getString(a,3),is("two"));
    Assert.assertThat(f9.get(a,0),is((byte)2));
    Assert.assertThat(f9.get(a,1),is((byte)2));
    Assert.assertThat(f9.get(a,2),is((byte)2));

    // back to the beginning
    a=a.decrement(s1);
    s1.move(a,0,1,2);

    Assert.assertThat(f1.get(a),is((byte)1));
    Assert.assertThat(f2.get(a),is((short)1));
    Assert.assertThat(f3.get(a),is(1));
    Assert.assertThat(f4.get(a),is((long)1));
    Assert.assertThat(f5.get(a),is('a'));
    Assert.assertThat(f6.get(a),is(1.0f));
    Assert.assertThat(f7.get(a),is(1.0d));
    Assert.assertThat(f8.getString(a,3),is("one"));
    Assert.assertThat(f9.get(a,0),is((byte)1));
    Assert.assertThat(f9.get(a,1),is((byte)1));
    Assert.assertThat(f9.get(a,2),is((byte)1));

    a=a.increment(s1);
    Assert.assertThat(f1.get(a),is((byte)1));
    Assert.assertThat(f2.get(a),is((short)1));
    Assert.assertThat(f3.get(a),is(1));
    Assert.assertThat(f4.get(a),is((long)1));
    Assert.assertThat(f5.get(a),is('a'));
    Assert.assertThat(f6.get(a),is(1.0f));
    Assert.assertThat(f7.get(a),is(1.0d));
    Assert.assertThat(f8.getString(a,3),is("one"));
    Assert.assertThat(f9.get(a,0),is((byte)1));
    Assert.assertThat(f9.get(a,1),is((byte)1));
    Assert.assertThat(f9.get(a,2),is((byte)1));

    a=a.increment(s1);
    Assert.assertThat(f1.get(a),is((byte)2));
    Assert.assertThat(f2.get(a),is((short)2));
    Assert.assertThat(f3.get(a),is(2));
    Assert.assertThat(f4.get(a),is((long)2));
    Assert.assertThat(f5.get(a),is('b'));
    Assert.assertThat(f6.get(a),is(2.0f));
    Assert.assertThat(f7.get(a),is(2.0d));
    Assert.assertThat(f8.getString(a,3),is("two"));
    Assert.assertThat(f9.get(a,0),is((byte)2));
    Assert.assertThat(f9.get(a,1),is((byte)2));
    Assert.assertThat(f9.get(a,2),is((byte)2));

    // back
    a=a.decrement(s1, 2);

    s1.move(a,1,0,2);

    Assert.assertThat(f1.get(a),is((byte)1));
    Assert.assertThat(f2.get(a),is((short)1));
    Assert.assertThat(f3.get(a),is(1));
    Assert.assertThat(f4.get(a),is((long)1));
    Assert.assertThat(f5.get(a),is('a'));
    Assert.assertThat(f6.get(a),is(1.0f));
    Assert.assertThat(f7.get(a),is(1.0d));
    Assert.assertThat(f8.getString(a,3),is("one"));
    Assert.assertThat(f9.get(a,0),is((byte)1));
    Assert.assertThat(f9.get(a,1),is((byte)1));
    Assert.assertThat(f9.get(a,2),is((byte)1));

    a=a.increment(s1);
    Assert.assertThat(f1.get(a),is((byte)2));
    Assert.assertThat(f2.get(a),is((short)2));
    Assert.assertThat(f3.get(a),is(2));
    Assert.assertThat(f4.get(a),is((long)2));
    Assert.assertThat(f5.get(a),is('b'));
    Assert.assertThat(f6.get(a),is(2.0f));
    Assert.assertThat(f7.get(a),is(2.0d));
    Assert.assertThat(f8.getString(a,3),is("two"));
    Assert.assertThat(f9.get(a,0),is((byte)2));
    Assert.assertThat(f9.get(a,1),is((byte)2));
    Assert.assertThat(f9.get(a,2),is((byte)2));

    a=a.increment(s1);
    Assert.assertThat(f1.get(a),is((byte)2));
    Assert.assertThat(f2.get(a),is((short)2));
    Assert.assertThat(f3.get(a),is(2));
    Assert.assertThat(f4.get(a),is((long)2));
    Assert.assertThat(f5.get(a),is('b'));
    Assert.assertThat(f6.get(a),is(2.0f));
    Assert.assertThat(f7.get(a),is(2.0d));
    Assert.assertThat(f8.getString(a,3),is("two"));
    Assert.assertThat(f9.get(a,0),is((byte)2));
    Assert.assertThat(f9.get(a,1),is((byte)2));
    Assert.assertThat(f9.get(a,2),is((byte)2));
  }
}
