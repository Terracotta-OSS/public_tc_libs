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

import com.terracottatech.sovereign.impl.ManagedAction;
import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.impl.compute.IndexedCellRangePredicate;
import com.terracottatech.sovereign.impl.memory.ContextImpl;
import com.terracottatech.sovereign.impl.memory.PersistentMemoryLocator;
import com.terracottatech.sovereign.impl.model.SovereignContainer;
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.BiFunction;

/**
 * Generates Spliterator based on whether records are scanned directly (full dataset scan) or
 * through primary or secondary indexes. The {@link CellComparison} object specifies which index
 * to use, based on the predicate in the query. A null {@code CellComparison} denotes that
 * an index cannot be used to scan the dataset.
 *
 * @param <K> record key type
 *
 * @author mscott
 */
public class LocatorGenerator<K extends Comparable<K>> {
  private final Catalog<K> catalog;

  public LocatorGenerator(Catalog<K> catalog) {
    this.catalog = catalog;
  }

  <V extends Comparable<V>> boolean isIndexed(CellDefinition<V> cellDefinition) {
    this.testDisposed();
    return catalog.hasSortedIndex(cellDefinition);
  }

  public <V extends Comparable<V>> long getIndexCoverageEstimate(CellDefinition<V> cellDefinition) {
    return (catalog.hasSortedIndex(cellDefinition)) ? catalog.getSortedIndexFor(cellDefinition).estimateSize() : Long.MAX_VALUE;
  }

  public Spliterator<Record<K>> createSpliterator(RecordStreamImpl<K> stream, CellComparison<K> driver, ManagedAction<K> managed) {
    this.testDisposed();
    ContextImpl c = catalog.getContainer().start(true);
    stream.associateCloseable(c);

    long msn = catalog.getCurrentMSN();
    if (driver == null) {
      PersistentMemoryLocator loc = catalog.getContainer().first(c);
      final ManagedSpliterator<K> spliterator = new ManagedSpliterator<>(stream, catalog.getContainer(), loc,
              msn,
              r -> true,
              managed);
      stream.associateCloseable(spliterator);
      return spliterator;
    } else {
      LocatorSupplier locatorSupplier = new LocatorSupplier(c, driver);
      locatorSupplier.generateFromCellComparison();
      final ManagedSpliterator<K> spliterator = (driver.countIndexRanges() <= 0) ?
          new ManagedSpliterator<>(stream, catalog.getContainer(), locatorSupplier.getLocator(), msn,
            locatorSupplier.getEndpoint(), managed) :
          new ManagedMultiScanSpliterator<>(stream, catalog.getContainer(), locatorSupplier.getLocator(), msn,
            locatorSupplier.getEndpoint(), locatorSupplier::generateFromCellComparison,  managed);
      stream.associateCloseable(spliterator);
      return spliterator;
    }
  }

  public Spliterator<Record<K>> createSpliterator(RecordStreamImpl<K> stream, CellComparison<K> driver) {
    this.testDisposed();
    if (driver == null) {
      return createSpliterator(stream);
    } else {
      ContextImpl c = catalog.getContainer().start(true);
      stream.associateCloseable(c);

      LocatorSupplier locatorSupplier = new LocatorSupplier(c, driver);
      locatorSupplier.generateFromCellComparison();
      final ManagedSpliterator<K> spliterator = new ManagedSpliterator<>(stream, catalog.getContainer(),
                                                                         locatorSupplier.getLocator(),
                                                                         catalog.getCurrentMSN(),
                                                                         locatorSupplier.getEndpoint(), null);
      stream.associateCloseable(spliterator);

      return spliterator;
    }
  }

  public Spliterator<Record<K>> createSpliterator(final RecordStreamImpl<K> stream) {
    this.testDisposed();
    final SovereignContainer<K> dataContainer = catalog.getContainer();
    final ContextImpl c = dataContainer.start(true);
    stream.associateCloseable(c);
    return new RecordSpliterator<>(stream, dataContainer.first(c), dataContainer, catalog.getCurrentMSN());
  }

  private void testDisposed() throws IllegalStateException {
    if (this.catalog.isDisposed()) {
      throw new IllegalStateException("Attempt to use disposed dataset");
    }
  }

  /**
   * Encapsulate the locator generator that generates location based on the current Index fragment.
   * The method {@link #generateFromCellComparison()} is used to supply generated locators to
   * calling spliterators (see {@link ManagedMultiScanSpliterator}).
   */
  private final class LocatorSupplier implements IndexRange<K> {
    private final ContextImpl context;
    private final Iterator<IndexedCellRangePredicate<K, ? extends Comparable<?>>> fragmentIterator;

    private IndexedCellRangePredicate<K, ? extends Comparable<?>> currentFragment;
    private PersistentMemoryLocator currentLocator;

    LocatorSupplier(ContextImpl c, CellComparison<K> cellComparison) {
      this.context  = c;
      this.fragmentIterator = cellComparison.indexRangeIterator();
    }

    private IndexRange<K> generateFromCellComparison() {
      if (!fragmentIterator.hasNext()) {
        return null;
      }
      this.currentFragment = fragmentIterator.next();
      this.currentLocator = getCurrentLocator(currentFragment);
      return this;
    }

    /**
     * Note: generic type conversion without a warning.
     */
    private <V extends Comparable<V>> PersistentMemoryLocator getCurrentLocator(IndexedCellRangePredicate<K, V> fragment) {
      return getCurrentLocatorAccessor(fragment).apply(context, fragment.start());
    }

    private <V extends Comparable<V>> BiFunction<ContextImpl, V, PersistentMemoryLocator> getCurrentLocatorAccessor(IndexedCellRangePredicate<K, V> fragment) {
      SovereignSortedIndexMap<V, K> indexMap = catalog.getSortedIndexFor(fragment.getCellDefinition());
      switch (fragment.operation()) {
        case EQ:
          return indexMap::get;
        case GREATER_THAN_OR_EQUAL:
          return indexMap::higherEqual;
        case GREATER_THAN:
          return indexMap::higher;
        case LESS_THAN:
          return indexMap::lower;
        case LESS_THAN_OR_EQUAL:
          return indexMap::lowerEqual;
        default:
          throw new UnsupportedOperationException("bad type: " + fragment.operation());
      }
    }

    @Override
    public PersistentMemoryLocator getLocator() {
      return currentLocator;
    }

    @Override
    public RangePredicate<K> getEndpoint() {
      return currentFragment;
    }
  }
}
