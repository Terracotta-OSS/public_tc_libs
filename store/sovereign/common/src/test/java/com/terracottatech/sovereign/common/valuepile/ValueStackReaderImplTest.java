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

package com.terracottatech.sovereign.common.valuepile;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 10/3/2016.
 */
public class ValueStackReaderImplTest {

  @Test
  public void testBoolean() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneBoolean(true);
    rw.oneBoolean(false);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneBoolean(), is(true));
    assertThat(rr.oneBoolean(), is(false));
  }

  @Test
  public void testChar() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneChar('q');
    rw.oneChar(' ');
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneChar(), is('q'));
    assertThat(rr.oneChar(), is(' '));
  }

  @Test
  public void testByte() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneByte((byte) -120);
    rw.oneByte((byte) 10);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneByte(), is((byte) -120));
    assertThat(rr.oneByte(), is((byte) 10));
  }

  @Test
  public void testShort() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneShort(Short.MAX_VALUE / 4);
    rw.oneShort(-10);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneShort(), is((short) (Short.MAX_VALUE / 4)));
    assertThat(rr.oneShort(), is((short) (-10)));
  }

  @Test
  public void testInt() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneInt(Integer.MAX_VALUE / 4);
    rw.oneInt(-10);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneInt(), is(Integer.MAX_VALUE / 4));
    assertThat(rr.oneInt(), is((-10)));
  }

  @Test
  public void testLong() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneLong(Long.MAX_VALUE / 2);
    rw.oneLong(-9223372036854775807l);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneLong(), is(Long.MAX_VALUE / 2));
    assertThat(rr.oneLong(), is(-9223372036854775807l));
  }

  @Test
  public void testFloat() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneFloat(1.1f);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneFloat(), is(1.1f));
  }

  @Test
  public void testDouble() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.oneDouble(1.1d);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);
    assertThat(rr.oneDouble(), is(1.1d));
  }

  @Test
  public void testByteArray() throws Exception {
    byte[] barr1 = new byte[] { 0x1f, 0x0c, 0x07, 0x17 };
    byte[] barr2 = new byte[] { 0x0f, 0x1c, 0x17 };
    byte[] barr3=new byte[0];

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.bytes(barr1, 0, barr1.length);
    rw.bytes(barr2, 0, barr2.length);
    rw.bytes(barr3, 0, barr3.length);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);

    assertThat(rr.bytes(), equalTo(ByteBuffer.wrap(barr1)));
    assertThat(rr.bytes(), equalTo(ByteBuffer.wrap(barr2)));
    assertThat(rr.bytes(), equalTo(ByteBuffer.wrap(barr3)));
  }

  @Test
  public void testString() throws Exception {
    String text1 = "test ascii";
    String text2 = "test \uD801\uDC00";
    String text3 = "test \u0301\u0300";
    String text4 = "";

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.utfString(text1);
    rw.utfString(text2);
    rw.utfString(text3);
    rw.utfString(text4);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);

    assertThat(rr.utfString(), is(text1));
    assertThat(rr.utfString(), is(text2));
    assertThat(rr.utfString(), is(text3));
    assertThat(rr.utfString(), is(text4));

    assertThat(rr.utfString(0), is(text1));
    assertThat(rr.utfString(1), is(text2));
    assertThat(rr.utfString(2), is(text3));
    assertThat(rr.utfString(3), is(text4));
  }

  @Test
  public void testEncodedInts() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ValuePileWriterImpl rw = new ValuePileWriterImpl(baos, 0);
    rw.encodedInt(1);
    rw.encodedInt(2);
    rw.encodedInt(3);
    rw.encodedInt(128);
    rw.encodedInt(Integer.MAX_VALUE / 8);
    rw.encodedInt(Integer.MAX_VALUE);
    rw.encodedInt(Integer.MIN_VALUE);
    rw.finish();
    ValuePileReaderImpl rr = new ValuePileReaderImpl(ByteBuffer.wrap(baos.toByteArray()), 0);

    assertThat(rr.encodedInt(), is(1));
    assertThat(rr.encodedInt(), is(2));
    assertThat(rr.encodedInt(), is(3));
    assertThat(rr.encodedInt(), is(128));
    assertThat(rr.encodedInt(), is(Integer.MAX_VALUE / 8));
    assertThat(rr.encodedInt(), is(Integer.MAX_VALUE));
    assertThat(rr.encodedInt(), is(Integer.MIN_VALUE));
  }

}