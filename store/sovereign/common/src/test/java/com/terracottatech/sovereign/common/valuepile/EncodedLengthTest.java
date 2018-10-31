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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 10/3/2016.
 */
public class EncodedLengthTest {

  @Test
  public void testNegative() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    int len= EncodedInteger.write(-10, baos);
    assertThat(len, is(5));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(-10));
  }

  @Test
  public void testTiny() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    int len= EncodedInteger.write(34, baos);
    assertThat(len, is(1));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(34));
  }

  @Test
  public void testShort() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    int len= EncodedInteger.write(Short.MAX_VALUE/4, baos);
    assertThat(len, is(2));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(Short.MAX_VALUE/4));
  }

  @Test
  public void testPackedInt() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    int len= EncodedInteger.write(Integer.MAX_VALUE/4, baos);
    assertThat(len, is(4));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(Integer.MAX_VALUE/4));
  }


  @Test
  public void test128() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    int len= EncodedInteger.write(128, baos);
    assertThat(len, is(2));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(128));
  }

  @Test
  public void testUnPackedInt() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int len = EncodedInteger.write(Integer.MAX_VALUE, baos);
    assertThat(len, is(5));
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    int p = EncodedInteger.read(is);
    assertThat(p, is(Integer.MAX_VALUE));
  }

  @Test
  public void testEncodedEdges() throws Exception {
    ByteArrayOutputStream baos=new ByteArrayOutputStream();
    EncodedInteger.write(Integer.MAX_VALUE, baos);
    EncodedInteger.write(Integer.MIN_VALUE, baos);
    EncodedInteger.write(0, baos);
    ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
    assertThat(EncodedInteger.read(is), is(Integer.MAX_VALUE));
    assertThat(EncodedInteger.read(is), is(Integer.MIN_VALUE));
    assertThat(EncodedInteger.read(is), is(0));
  }
}