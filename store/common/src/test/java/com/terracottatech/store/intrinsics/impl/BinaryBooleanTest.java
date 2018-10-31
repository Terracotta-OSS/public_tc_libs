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

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link BinaryBoolean} classes.
 */
@SuppressWarnings("unchecked")
public class BinaryBooleanTest {

  private final IntCellDefinition ageCell = CellDefinition.defineInt("age");
  private final StringCellDefinition nameCell = CellDefinition.defineString("name");

  @Test
  public void testAnd() {
    BinaryBoolean.And<Record<?>> and = and(0, "a");
    assertEquals(and, and);
    assertEquals(and.hashCode(), and.hashCode());

    BinaryBoolean.And<Record<?>> same = and(0, "a");
    assertEquals(and, same);
    assertEquals(and.hashCode(), same.hashCode());

    BinaryBoolean.And<Record<?>> flipped = new BinaryBoolean.And<>(
            (IntrinsicPredicate<Record<?>>) and.getRight(),
            and.getLeft()
    );
    assertNotEquals(and, flipped);

    BinaryBoolean.And<Record<?>> other = and(-1, "b");
    assertNotEquals(and, other);

    BinaryBoolean.Or<Record<?>> or = or(0, "a");
    assertNotEquals(and, or);
  }

  @Test
  public void testOr() {
    BinaryBoolean.Or<Record<?>> or = or(0, "a");
    assertEquals(or, or);
    assertEquals(or.hashCode(), or.hashCode());

    BinaryBoolean.Or<Record<?>> same = or(0, "a");
    assertEquals(or, same);
    assertEquals(or.hashCode(), same.hashCode());

    BinaryBoolean.Or<Record<?>> flipped = new BinaryBoolean.Or<>(
            (IntrinsicPredicate<Record<?>>) or.getRight(),
            or.getLeft()
    );
    assertNotEquals(or, flipped);

    BinaryBoolean.Or<Record<?>> other = or(-1, "b");
    assertNotEquals(or, other);

    BinaryBoolean.And<Record<?>> and = and(0, "a");
    assertNotEquals(or, and);
  }

  private BinaryBoolean.And<Record<?>> and(int age, String name) {
    return new BinaryBoolean.And<>(
            (IntrinsicPredicate<Record<?>>) ageCell.value().isGreaterThan(age),
            (IntrinsicPredicate<Record<?>>) nameCell.value().is(name)
    );
  }

  private BinaryBoolean.Or<Record<?>> or(int age, String name) {
    return new BinaryBoolean.Or<>(
            (IntrinsicPredicate<Record<?>>) ageCell.value().isGreaterThan(age),
            (IntrinsicPredicate<Record<?>>) nameCell.value().is(name)
    );
  }
}
