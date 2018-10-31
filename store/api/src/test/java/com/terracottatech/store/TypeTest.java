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
package com.terracottatech.store;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author cdennis
 */
public class TypeTest {

  @Test
  public void testForJdkTypeReturnsNullOnUnknown() {
    assertThat(Type.forJdkType(Date.class), nullValue());
  }

  @Test
  public void testForJdkTypeOnDirectClasses() {
    assertThat(Type.forJdkType(Boolean.class), is(Type.BOOL));
    assertThat(Type.forJdkType(Character.class), is(Type.CHAR));
    assertThat(Type.forJdkType(Integer.class), is(Type.INT));
    assertThat(Type.forJdkType(Long.class), is(Type.LONG));
    assertThat(Type.forJdkType(Double.class), is(Type.DOUBLE));
    assertThat(Type.forJdkType(String.class), is(Type.STRING));
    assertThat(Type.forJdkType(byte[].class), is(Type.BYTES));
  }

  @Test
  @Ignore
  public void testForJdkTypeOnPrimitives() {
    assertThat(Type.forJdkType(Boolean.TYPE), is(Type.BOOL));
    assertThat(Type.forJdkType(Character.TYPE), is(Type.CHAR));
    assertThat(Type.forJdkType(Integer.TYPE), is(Type.INT));
    assertThat(Type.forJdkType(Long.TYPE), is(Type.LONG));
    assertThat(Type.forJdkType(Double.TYPE), is(Type.DOUBLE));
  }

  @Test
  public void testEqualsForBoolean() {
    assertTrue(Type.equals(Boolean.TRUE, Boolean.TRUE));
    assertFalse(Type.equals(Boolean.TRUE, Boolean.FALSE));
  }

  @Test
  public void testEqualsForCharacter() {
    assertTrue(Type.equals('#', '#'));
    assertFalse(Type.equals('#', '*'));
  }

  @Test
  public void testEqualsForInteger() {
    assertTrue(Type.equals(42, 42));
    assertFalse(Type.equals(42, 43));
  }

  @Test
  public void testEqualsForLong() {
    assertTrue(Type.equals(42L, 42L));
    assertFalse(Type.equals(42L, 43L));
  }

  @Test
  public void testEqualsForDouble() {
    assertTrue(Type.equals(1.0d, 1.0d));
    assertFalse(Type.equals(1.0d, 2.0d));
  }

  @Test
  public void testEqualsForString() {
    assertTrue(Type.equals("foo", new String("foo")));
    assertFalse(Type.equals("foo", new String("fooo")));
    assertFalse(Type.equals("foo", new String("for")));
  }

  @Test
  public void testEqualsForBytes() {
    assertTrue(Type.equals(new byte[] {0,1,2}, new byte[] {0,1,2}));
    assertFalse(Type.equals(new byte[] {0,1,2}, new byte[] {0,1,2,3}));
    assertFalse(Type.equals(new byte[] {0,1,2}, new byte[] {0,1,3}));
  }
}
