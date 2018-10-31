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
package com.terracottatech.store.intrinsics.impl;

import org.junit.Test;

import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.NEQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link NonGatedComparison} classes.
 */
public class NonGatedComparisonObjectMethodsTest {

  @Test
  public void testEquals() {
    Object equals = new NonGatedComparison.Equals<>(new Constant<>(1L),
            new Constant<>(1L));
    assertEquals(equals, equals);
    assertEquals(equals.hashCode(), equals.hashCode());

    Object same = new NonGatedComparison.Equals<>(new Constant<>(1L),
            new Constant<>(1L));
    assertEquals(equals, same);
    assertEquals(equals.hashCode(), same.hashCode());

    Object otherRight = new NonGatedComparison.Equals<>(new Constant<>(1L),
            new Constant<>(2L));
    assertNotEquals(equals, otherRight);

    Object otherLeft = new NonGatedComparison.Equals<>(new Constant<>(2L),
            new Constant<>(1L));
    assertNotEquals(equals, otherLeft);

    Object contrast = new NonGatedComparison.Contrast<>(new Constant<>(1L),
            EQ, new Constant<>(1L));
    assertNotEquals(equals, contrast);

    Object nanValue = new NonGatedComparison.Equals<>(new Constant<>(Double.NaN),
            new Constant<>(Double.NaN));
    assertNotEquals(equals, nanValue);

    Object anotherNanValue = new NonGatedComparison.Equals<>(new Constant<>(Double.NaN),
            new Constant<>(Double.NaN));
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }

  @Test
  public void testContrast() {
    Object contrast = new NonGatedComparison.Contrast<>(new Constant<>(1L),
            EQ, new Constant<>(1L));
    assertEquals(contrast, contrast);
    assertEquals(contrast.hashCode(), contrast.hashCode());

    Object same = new NonGatedComparison.Contrast<>(new Constant<>(1L),
            EQ, new Constant<>(1L));
    assertEquals(contrast, same);
    assertEquals(contrast.hashCode(), same.hashCode());

    Object otherRight = new NonGatedComparison.Contrast<>(new Constant<>(1L),
            EQ, new Constant<>(2L));
    assertNotEquals(contrast, otherRight);

    Object otherLeft = new NonGatedComparison.Contrast<>(new Constant<>(2L),
            EQ, new Constant<>(1L));
    assertNotEquals(contrast, otherLeft);

    Object otherType = new NonGatedComparison.Contrast<>(new Constant<>(1L),
            NEQ, new Constant<>(1L));
    assertNotEquals(contrast, otherType);

    Object equals = new NonGatedComparison.Equals<>(new Constant<>(1L),
            new Constant<>(1L));
    assertNotEquals(contrast, equals);

    Object nanValue = new NonGatedComparison.Contrast<>(new Constant<>(Double.NaN),
            EQ, new Constant<>(Double.NaN));
    assertNotEquals(equals, nanValue);

    Object anotherNanValue = new NonGatedComparison.Contrast<>(new Constant<>(Double.NaN),
            EQ, new Constant<>(Double.NaN));
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }
}
