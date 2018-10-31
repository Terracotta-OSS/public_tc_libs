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
package com.terracottatech.store.embedded.persistence.demo;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder;
import com.terracottatech.store.configuration.MemoryUnit;


import java.nio.file.Path;

import static com.terracottatech.store.definition.CellDefinition.define;
import static com.terracottatech.store.Type.LONG;

/**
 * TC Store sample manipulating a disk-resident store.
 */
// The formatting in this class is influenced by the inclusion of fragments from this class in documentation
public class EmbeddedPersistenceDemo {

  public void runPersistentDatasetCreateRetrieve(Path dataPath) throws Exception {

    try (
        // tag::persistentDataSetCreation1[]
        DatasetManager datasetManager = DatasetManager.embedded()
            .disk("disk", dataPath, // <1>
                EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY, // <2>
                EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW) // <3>
            .offheap("offheap", 64, MemoryUnit.MB)
            .build())
        // end::persistentDataSetCreation1[]
    {

      // tag::persistentDataSetCreation2[]
      DatasetConfiguration configuration = datasetManager.datasetConfiguration().offheap("offheap").disk("disk").build();  // <4>
      datasetManager.newDataset("sampleDataset", Type.STRING, configuration); // <5>
      // end::persistentDataSetCreation2[]


      try (
        // tag::persistentDataSetCreation3[]
        Dataset<String> sampleDataset = datasetManager.getDataset("sampleDataset", Type.STRING) // <6>
        // end::persistentDataSetCreation3[]
      ){

        // tag::basicPersistentMutation[]
        CellDefinition<Long> counter = define("counter", LONG);
        DatasetWriterReader<String> access =
            sampleDataset.writerReader(); // <1>
        access.add("dummyKey", counter.newCell(5L));
        // end::basicPersistentMutation[]
      }
    }

    try(DatasetManager datasetManager = DatasetManager.embedded()
        .disk("disk", dataPath,
                EmbeddedDatasetManagerBuilder.PersistenceMode.INMEMORY,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
        .offheap("offheap", 64, MemoryUnit.MB)
        .build()) {

      try(Dataset<String> dummyDataset = datasetManager.getDataset("sampleDataset", Type.STRING)) {
        DatasetReader<String> access = dummyDataset.reader();

        Record<String> rec2 = access.get("dummyKey").orElseThrow(AssertionError::new);
        System.out.println(rec2);
      }
    }
  }

  public void runPersistentHybridDatasetCreateRetrieve(Path dataPath) throws Exception {

    try (DatasetManager datasetManager = DatasetManager.embedded()
        .disk("disk", dataPath,
                EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN_OR_NEW)
        .offheap("offheap", 64, MemoryUnit.MB)
        .build()) {

      datasetManager.newDataset("sampleDataset", Type.STRING, datasetManager.datasetConfiguration()
                            .offheap("offheap")
                            .disk("disk")
                            .build());
      try(Dataset<String> dummyDataset = datasetManager.getDataset("sampleDataset", Type.STRING)) {
        CellDefinition<Long> counter = define("counter", LONG);
        DatasetWriterReader<String> access =
            dummyDataset.writerReader();
        access.add("dummyKey", counter.newCell(5L));
      }
    }

    try (DatasetManager datasetManager = DatasetManager.embedded()
        .disk("disk", dataPath,
                EmbeddedDatasetManagerBuilder.PersistenceMode.HYBRID,
            EmbeddedDatasetManagerBuilder.FileMode.REOPEN)
        .offheap("offheap", 64, MemoryUnit.MB)
        .build()) {

      try(Dataset<String> dummyDataset = datasetManager.getDataset("sampleDataset", Type.STRING)) {
        DatasetReader<String> access = dummyDataset.reader();
        Record<String> rec2 = access.get("dummyKey").orElseThrow(AssertionError::new);
        System.out.println(rec2);
      }
    }
  }
}
