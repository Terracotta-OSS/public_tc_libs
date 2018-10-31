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
package com.terracottatech.store.client;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.configuration.AdvancedDatasetConfigurationBuilder;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.test.data.Animals;
import com.terracottatech.test.data.Animals.Animal;
import com.terracottatech.test.data.Animals.Schema;

import java.util.function.BiFunction;

/**
 * Provides methods for creating a dataset
 * populated with {@link Animal Animal} records.
 *
 * @author Clifford W. Johnson
 */
public class AnimalsDataset {
  private AnimalsDataset() {
  }

  public static final class Container implements AutoCloseable {
    private final DatasetManager datasetManager;
    private final Dataset<String> dataset;

    private Container(DatasetManager datasetManager, Dataset<String> dataset) {
      this.datasetManager = datasetManager;
      this.dataset = dataset;
    }

    public DatasetManager getDatasetManager() {
      return datasetManager;
    }

    public Dataset<String> getDataset() {
      return dataset;
    }

    @Override
    public void close() {
      if (dataset != null) {
        dataset.close();
      }
      if (datasetManager != null) {
        datasetManager.close();
      }
    }
  }

  /**
   * Creates an <i>embedded</i> {@link Dataset} containing records formed from {@link Animals#ANIMALS}.
   * The returned dataset has indexes over {@link Schema#TAXONOMIC_CLASS TAXONOMIC_CLASS} and
   * {@link Schema#STATUS STATUS}.
   *
   * @param offHeap the amount of off-heap storage to use for the dataset
   * @param units the units for {@code offHeap}
   *
   * @return a new {@code Container} holding a reference to the {@link DatasetManager} and {@link Dataset} created;
   *        {@code Container} is {@link AutoCloseable} permitting the closure of both the {@code Dataset} and
   *        {@code DatasetManager} with a single call
   *
   * @throws StoreException if an error occurs while creating the dataset
   *
   * @see Animal#addTo(BiFunction)
   */
  public static Container createDataset(long offHeap, MemoryUnit units) throws StoreException {
    DatasetManager datasetManager = DatasetManager.embedded().offheap("offheap", offHeap, units).build();
    DatasetConfigurationBuilder configurationBuilder = datasetManager.datasetConfiguration()
        .offheap("offheap")
        .index(Schema.TAXONOMIC_CLASS, IndexSettings.BTREE)
        .index(Schema.STATUS, IndexSettings.BTREE);
    configurationBuilder = ((AdvancedDatasetConfigurationBuilder) configurationBuilder).concurrencyHint(2);
    DatasetConfiguration configuration = configurationBuilder.build();

    return new Container(datasetManager, createDataset("animals", datasetManager, configuration));
  }

  /**
   * Creates a {@link Dataset} containing records frormed from {@link Animals#ANIMALS} and using the
   * {@link DatasetManager} and {@link DatasetConfiguration} provided.
   *
   * @param datasetId the name for the new {@code Dataset}
   * @param datasetManager the {@code DatasetManager} through which the {@code Dataset} is constructed
   * @param configuration the {@code DatasetConfiguration} to use in creating the {@code Dataset}
   *
   * @return the new {@code Dataset}
   *
   * @throws StoreException if an error occurs while creating the dataset
   */
  public static Dataset<String> createDataset(String datasetId, DatasetManager datasetManager, DatasetConfiguration configuration)
      throws StoreException {
    datasetManager.newDataset(datasetId, Type.STRING, configuration);
    Dataset<String> dataset = datasetManager.getDataset(datasetId, Type.STRING);

    DatasetWriterReader<String> writerReader =
        dataset.writerReader();
    Animals.ANIMALS.forEach(animal -> animal.addTo(writerReader::add));

    return dataset;
  }
}
