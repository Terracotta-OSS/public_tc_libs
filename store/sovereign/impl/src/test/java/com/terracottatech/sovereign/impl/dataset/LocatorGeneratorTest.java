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

package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.AnimalsDataset;
import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.plan.IndexedCellRange;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import org.junit.Before;
import org.junit.Test;

import java.util.Spliterator;

import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author Clifford W. Johnson
 * @author RKAV modified for SovereignOptimizer
*/
public class LocatorGeneratorTest {

  private LocatorGenerator<String> factory;
  private SovereignDatasetImpl<String> dataset;

  @Before
  public void setup() throws Exception {
    this.dataset = (SovereignDatasetImpl<String>)AnimalsDataset.createDataset(16*1024 * 1024);
    AnimalsDataset.addIndexes(this.dataset);
    this.factory = new LocatorGenerator<>(this.dataset);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testDisposedIsIndexed() {
    final CellComparison<String> cellComparison = getFromOptimizer();
    assertNotNull(cellComparison);
    cellComparison.indexRanges()
            .stream()
            .map(IndexedCellRange::getCellDefinition)
            .forEach(def -> assertTrue(factory.isIndexed(def)));
    this.dataset.dispose();
    try {
      CellDefinition<Integer> mock = mock(CellDefinition.class);
      this.factory.isIndexed(mock);
      fail();
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testDisposedCreateSpliterator1Arg() {
    try (RecordStreamImpl<String> stream = (RecordStreamImpl<String>)this.dataset.records()) {
      assertNotNull(this.factory.createSpliterator(stream));

      this.dataset.dispose();
      try {
        this.factory.createSpliterator(stream);
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
    }
  }

  @Test
  public void testDisposedCreateSpliterator2Arg() {
    final CellComparison<String> cellComparison = getFromOptimizer();
    assertNotNull(cellComparison);

    try (final RecordStreamImpl<String> stream = (RecordStreamImpl<String>)RecordStreamImpl.newInstance(this.dataset, false)) {
      assertNotNull(this.factory.createSpliterator(stream, cellComparison));

      this.dataset.dispose();
      try {
        this.factory.createSpliterator(stream, cellComparison);
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
    }
  }

  @Test
  public void testDisposedCreateSpliterator3Arg() {
    final CellComparison<String> cellComparison = getFromOptimizer();
    assertNotNull(cellComparison);

    try (final RecordStreamImpl<String> stream = (RecordStreamImpl<String>)RecordStreamImpl.newInstance(this.dataset, false)) {
      final ManagedAction<String> action = LocatorGeneratorTest.this.dataset::getContainer;

      assertNotNull(this.factory.createSpliterator(stream, cellComparison, action));

      this.dataset.dispose();
      try {
        this.factory.createSpliterator(stream, cellComparison, action);
        fail();
      } catch (IllegalStateException e) {
        // Expected
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpliterator3ArgClose() throws Exception {
    final CellComparison<String> cellComparison = getFromOptimizer();
    assertNotNull(cellComparison);

    final MockAction mockAction = mock(MockAction.class);
    when(mockAction.getContainer()).thenReturn((SovereignContainer) this.dataset.getContainer());   // unchecked
    try (final RecordStreamImpl<String> stream = (RecordStreamImpl<String>)RecordStreamImpl.newInstance(this.dataset, false)) {
      final Spliterator<Record<String>> spliterator =
          this.factory.createSpliterator(stream, cellComparison, mockAction);
      assertNotNull(spliterator);
    }

    verify(mockAction).close();
  }

  @SuppressWarnings("unchecked")
  private CellComparison<String> getFromOptimizer() {
    BuildablePredicate<Record<?>> expression = TAXONOMIC_CLASS.value().isGreaterThan("");
    IntrinsicPredicate<Record<String>> stringExpression = (IntrinsicPredicate<Record<String>>) (Object) expression;
    return SovereignOptimizerFactory.newInstance(this.factory)
            .optimize(stringExpression)
            .getCellComparison();
  }

  @SuppressWarnings("try")
  private interface MockAction extends ManagedAction<String>, AutoCloseable {
  }
}
