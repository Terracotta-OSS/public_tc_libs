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

import static com.terracottatech.store.intrinsics.impl.ComparisonType.EQ;
import static com.terracottatech.store.intrinsics.impl.ComparisonType.NEQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test equals and hashCode implementations in {@link GatedComparison} classes.
 */
@SuppressWarnings("unchecked")
public class GatedComparisonObjectMethodsTest {

  private final IntrinsicFunction<Record<?>, Optional<Double>> extractor =
          (IntrinsicFunction<Record<?>, Optional<Double>>) CellExtractor.extractComparable(
                  CellDefinition.defineDouble("cell"));

  private final IntrinsicFunction<Record<?>, Optional<Double>> sameExtractor =
          (IntrinsicFunction<Record<?>, Optional<Double>>) CellExtractor.extractComparable(
                  CellDefinition.defineDouble("cell"));

  private final IntrinsicFunction<Record<?>, Optional<Double>> otherExtractor =
          (IntrinsicFunction<Record<?>, Optional<Double>>) CellExtractor.extractComparable(
                  CellDefinition.defineDouble("anotherCell"));

  @Test
  public void testEquals() {
    Object equals = new GatedComparison.Equals<>(extractor,
            new Constant<>(1.0));
    assertEquals(equals, equals);
    assertEquals(equals.hashCode(), equals.hashCode());

    Object same = new GatedComparison.Equals<>(sameExtractor,
            new Constant<>(1.0));
    assertEquals(equals, same);
    assertEquals(equals.hashCode(), same.hashCode());

    Object otherRight = new GatedComparison.Equals<>(extractor,
            new Constant<>(2.0));
    assertNotEquals(equals, otherRight);

    Object otherLeft = new GatedComparison.Equals<>(otherExtractor,
            new Constant<>(1.0));
    assertNotEquals(equals, otherLeft);

    Object contrast = new GatedComparison.Contrast<>(extractor,
            EQ, new Constant<>(1.0));
    assertNotEquals(equals, contrast);

    Object nanValue = new GatedComparison.Equals<>(otherExtractor,
            new Constant<>(Double.NaN));
    assertNotEquals(equals, nanValue);

    Object anotherNanValue = new GatedComparison.Equals<>(otherExtractor,
            new Constant<>(Double.NaN));
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }

  @Test
  public void testContrast() {
    Object contrast = new GatedComparison.Contrast<>(extractor,
            EQ, new Constant<>(1.0));
    assertEquals(contrast, contrast);
    assertEquals(contrast.hashCode(), contrast.hashCode());

    Object same = new GatedComparison.Contrast<>(sameExtractor,
            EQ, new Constant<>(1.0));
    assertEquals(contrast, same);
    assertEquals(contrast.hashCode(), same.hashCode());

    Object otherRight = new GatedComparison.Contrast<>(extractor,
            EQ, new Constant<>(2.0));
    assertNotEquals(contrast, otherRight);

    Object otherLeft = new GatedComparison.Contrast<>(otherExtractor,
            EQ, new Constant<>(1.0));
    assertNotEquals(contrast, otherLeft);

    Object otherType = new GatedComparison.Contrast<>(extractor,
            NEQ, new Constant<>(1.0));
    assertNotEquals(contrast, otherType);

    Object equals = new GatedComparison.Equals<>(extractor,
            new Constant<>(1.0));
    assertNotEquals(contrast, equals);

    Object nanValue = new GatedComparison.Contrast<>(extractor,
            NEQ, new Constant<>(Double.NaN));
    assertNotEquals(equals, nanValue);

    Object anotherNanValue = new GatedComparison.Contrast<>(extractor,
            NEQ, new Constant<>(Double.NaN));
    assertEquals(nanValue, anotherNanValue);
    assertEquals(nanValue.hashCode(), anotherNanValue.hashCode());
  }
}
