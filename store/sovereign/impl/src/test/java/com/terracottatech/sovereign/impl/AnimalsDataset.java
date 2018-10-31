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
package com.terracottatech.sovereign.impl;

import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.VersionLimitStrategy;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.indexing.SovereignIndexing;
import com.terracottatech.sovereign.spi.store.DataContainer;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Type;
import com.terracottatech.test.data.Animals;
import com.terracottatech.tool.Diagnostics;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;

/**
 * Provides methods for creating a {@link com.terracottatech.sovereign.SovereignDataset SovereignDataset}
 * populated with {@link Animals.Animal Animal} records.
 *
 * @author Clifford W. Johnson
 */
public final class AnimalsDataset {

  /**
   * The default value for {@link SovereignBuilder#limitVersionsTo(int)} used
   * by {@link #createDataset(int)}.
   */
  private static final int RETAINED_VERSION_DEFAULT = 2;

  /**
   * Indicates whether or not a Java heap dump was taken due to a lack of off-heap space
   * for dataset allocation.
   */
  private static final AtomicBoolean ALLOCATION_HEAP_DUMP_TAKEN = new AtomicBoolean(false);

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private AnimalsDataset() {
  }

  /**
   * Creates an off-heap {@link SovereignDataset SovereignDataset}
   * populated with a collection of animals.  The cell definitions are found in
   * {@link Animals.Schema Schema} and a Java object which may be used to represent the record is
   * {@link Animals.Animal Animal}.
   * <p>By default, no indexes are created over the dataset.  The dataset is created using
   * a default of {@value #RETAINED_VERSION_DEFAULT} for {@link SovereignBuilder#limitVersionsTo(int)}
   * and the builder-supplied default for {@link SovereignBuilder#versionLimitStrategy(VersionLimitStrategy)}.
   *
   * @param maxSpace the maximum amount of space, in bytes, of off-heap space allocated for the database
   *
   * @return a new, populated {@code SovereignDataset}
   *
   * @see Animals.Schema
   * @see #addIndexes(SovereignDataset)
   * @see #addIndex(SovereignDataset, CellDefinition)
   */
  public static SovereignDataset<String> createDataset(final int maxSpace) {
    return createDataset(maxSpace, true);
  }

  /**
   * Creates an off-heap {@link SovereignDataset SovereignDataset}
   * populated with a collection of animals.  The cell definitions are found in
   * {@link Animals.Schema Schema} and a Java object
   * which may be used to represent the record is
   * {@link Animals.Animal Animal}.
   * <p>By default, no indexes are created over the dataset.
   *
   * @param maxSpace the maximum amount of space, in bytes, of off-heap space allocated for the database
   * @param permitCompositeContainer
   *      if {@code true}, sets {@link SovereignBuilder#limitVersionsTo(int)} to {@value #RETAINED_VERSION_DEFAULT}
   *      and uses the builder-supplied default for
   *      {@link SovereignBuilder#versionLimitStrategy(VersionLimitStrategy) versionLimitStrategy};
   *      if {@code false}, sets {@code SovereignBuilder} properties to values causing use of
   *      a non-composite {@link DataContainer}.
   *
   * @return a new, populated {@code SovereignDataset}
   *
   * @see Animals.Schema
   * @see #addIndexes(SovereignDataset)
   * @see #addIndex(SovereignDataset, CellDefinition)
   *
   * @throws AssertionError if {@code permitCompositeContainer} is {@code false} and the {@code SovereignDataset}
   *      allocated is based on a composite {@code DataContainer}
   */
  public static SovereignDataset<String> createDataset(final int maxSpace, final boolean permitCompositeContainer) {
    final SovereignBuilder<String, SystemTimeReference> builder =
        new SovereignBuilder<>(Type.STRING, SystemTimeReference.class)
            .offheap(maxSpace);

    if (permitCompositeContainer) {
      builder
          .limitVersionsTo(RETAINED_VERSION_DEFAULT);
    } else {
      builder
          .limitVersionsTo(1);
    }

    return buildDataset(builder);
  }

  /**
   * Creates a {@link SovereignDataset} populated with a collection of animals.
   * The cell definitions are found in {@link Animals.Schema Schema}
   * and a Java object which may be used to represent the record is
   * {@link Animals.Animal Animal}.
   * <p>By default, no indexes are created over the dataset.
   *
   * @param builder the {@code SovereignBuilder} instance to use for configuring the dataset
   *
   * @return a new, populated {@code SovereignDataset}
   */
  public static SovereignDataset<String> createDataset(final SovereignBuilder<String, SystemTimeReference> builder) {
    return buildDataset(builder);
  }

  /**
   * Gets a {@link SovereignBuilder} pre-configured for building the Animals dataset.
   *
   * @param maxSpace the maximum amount of space, in bytes, of off-heap space allocated for the database
   * @param permitCompositeContainer
   *      if {@code true}, sets {@link SovereignBuilder#limitVersionsTo(int)} to {@value #RETAINED_VERSION_DEFAULT};
   *      if {@code false}, sets {@code SovereignBuilder} properties to values causing use of
   *      a non-composite {@link DataContainer}.
   *
   * @return a new {@code SovereignBuilder}
   */
  public static SovereignBuilder<String, SystemTimeReference> getBuilder(final int maxSpace, final boolean permitCompositeContainer) {
    final SovereignBuilder<String, SystemTimeReference> builder =
        new SovereignBuilder<>(Type.STRING, SystemTimeReference.class).offheap(maxSpace);
    if (permitCompositeContainer) {
      builder
          .limitVersionsTo(RETAINED_VERSION_DEFAULT);
    } else {
      builder
          .limitVersionsTo(1);
    }
    return builder;
  }

  /**
   * Builds and loads an off-heap {@code SovereignDataset}.
   *
   * @param builder the {@code SovereignBuilder} to use for constructing the dataset
   *
   * @return a new, populated {@code SovereignDataset}
   *
   * @see #createDataset(int)
   */
  private static SovereignDataset<String> buildDataset(
      final SovereignBuilder<String, SystemTimeReference> builder) {
    final SovereignDataset<String> dataset;
    try {
      dataset = builder.build();
    } catch (IllegalArgumentException e) {
      final StackTraceElement[] stackTrace = e.getStackTrace();
      if (stackTrace[0].getClassName().equals(UpfrontAllocatingPageSource.class.getName())
          && stackTrace[0].getMethodName().equals("allocateBackingBuffers")) {
        if (!ALLOCATION_HEAP_DUMP_TAKEN.getAndSet(true)) {
          throw new AssertionError(String.format("Storage allocation error; created heap dump: %s%n",
              Diagnostics.dumpHeap(false)), e);
        }
      }
      throw e;
    }

    for (final Animals.Animal animal : Animals.ANIMALS) {
      animal.addTo((key, cells) -> dataset.add(SovereignDataset.Durability.IMMEDIATE, key, cells));
    }
    return dataset;
  }

  /**
   * Adds indexes for {@link Animals.Schema#TAXONOMIC_CLASS taxonomicClass}
   * and {@link Animals.Schema#STATUS statusDef} cells.  This method
   * uses synchronous index creation.
   *
   * @param dataset the {@code SovereignDataset} instance to which the indexes are added
   */
  public static void addIndexes(final SovereignDataset<?> dataset) throws Exception {
    final SovereignIndexing datasetIndexing = dataset.getIndexing();
    datasetIndexing.createIndex(TAXONOMIC_CLASS, SovereignIndexSettings.btree()).call();
    datasetIndexing.createIndex(STATUS, SovereignIndexSettings.btree()).call();
    // OBSERVATIONS is intentionally not indexed here
  }

  /**
   * Adds an index to a {@code SovereignDataset} over the {@code CellDefinition} specified.
   *
   * @param dataset the {@code SovereignDataset} instance to which the index is added
   * @param cellDefinition the {@code CellDefinition} over which the index is added
   * @param <V> the type of the {@code cellDefinition} values
   */
  @SuppressWarnings({ "unused", "WeakerAccess" })
  public static <K extends Comparable<K>, V extends Comparable<V>> void addIndex(
    final SovereignDataset<K> dataset, final CellDefinition<V> cellDefinition)
      throws Exception {
    Objects.requireNonNull(dataset, "dataset");
    Objects.requireNonNull(cellDefinition, "cellDefinition");

    final SovereignIndexing datasetIndexing = dataset.getIndexing();
    datasetIndexing.createIndex(cellDefinition, SovereignIndexSettings.btree()).call();
  }
}
