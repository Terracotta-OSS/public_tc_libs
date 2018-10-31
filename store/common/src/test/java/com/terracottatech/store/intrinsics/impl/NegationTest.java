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
import com.terracottatech.store.intrinsics.IntrinsicFunction;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link Negation} classes.
 */
public class NegationTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testNegation() {

    Object negation = new Negation<>(new GatedComparison.Equals<>(
            (IntrinsicFunction<Record<?>, Optional<Long>>) CellExtractor.extractComparable(
            CellDefinition.defineLong("longCell")),
            new Constant<>(1L)));
    assertEquals(negation, negation);
    assertEquals(negation.hashCode(), negation.hashCode());

    Object same = new Negation<>(new GatedComparison.Equals<>(
            (IntrinsicFunction<Record<?>, Optional<Long>>) CellExtractor.extractComparable(
            CellDefinition.defineLong("longCell")),
            new Constant<>(1L)));
    assertEquals(negation, same);
    assertEquals(negation.hashCode(), same.hashCode());

    Object otherLeft = new Negation<>(new GatedComparison.Equals<>(
            (IntrinsicFunction<Record<?>, Optional<Long>>) CellExtractor.extractComparable(
                    CellDefinition.defineLong("otherCell")),
            new Constant<>(1L)));
    assertNotEquals(negation, otherLeft);

    Object otherRight = new Negation<>(new GatedComparison.Equals<>(
            (IntrinsicFunction<Record<?>, Optional<Long>>) CellExtractor.extractComparable(
                    CellDefinition.defineLong("longCell")),
            new Constant<>(2L)));
    assertNotEquals(negation, otherRight);

    Object other = new Negation<>(new NonGatedComparison.Equals<>(new Constant<>(1L),
            new Constant<>(1L)));
    assertNotEquals(negation, other);
  }

}
