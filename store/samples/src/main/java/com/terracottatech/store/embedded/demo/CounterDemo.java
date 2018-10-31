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
package com.terracottatech.store.embedded.demo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.async.AsyncDatasetWriterReader;
import com.terracottatech.store.async.Operation;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.indexing.Index;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.stream.MutableRecordStream;

import java.io.PrintWriter;
import java.io.Writer;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

// tag::staticImports[]
import static com.terracottatech.store.UpdateOperation.allOf;
import static com.terracottatech.store.UpdateOperation.remove;
import static com.terracottatech.store.UpdateOperation.write;
// end::staticImports[]

/**
 * Demonstrate the Terracotta Store API with concrete examples.
 * <p>
 * Combined with the Terracotta Store "Getting Started with the Terracotta Store API" document, this
 * program demonstrates many capabilities of the Terracotta Store API.  Although simplistic, many
 * aspects of the API are demonstrated.  Please refer to the primary documentation and the
 * API javadocs for more details.
 *
 * <h3>Notes</h3>
 * <ul>
 *   <li>
 *     This program is written as a live example from which excerpts are made for the
 *     "Getting Started with the Terracotta Store API" document.  The organization of
 *     this program, including the documentation "markers", is geared toward document
 *     production.
 *   </li>
 * </ul>
 */
// The formatting in this class is influenced by the inclusion of fragments from this class in documentation
@SuppressFBWarnings("DM_DEFAULT_ENCODING")
public class CounterDemo {
  // tag::cellDefinitions[]
  LongCellDefinition counterCell = CellDefinition.defineLong("longCounter");
  BoolCellDefinition stoppedCell = CellDefinition.defineBool("stopped");
  StringCellDefinition stoppedByCell = CellDefinition.defineString("stoppedBy");
  // end::cellDefinitions[]

  /**
   * Runs the {@link CounterDemo} using an embedded {@link Dataset}.
   * @param args none
   * @throws StoreException if an embedded {@code DatasetManager} instance cannot be allocated
   */
  public static void main(String[] args) throws StoreException {
    CounterDemo demo = new CounterDemo();
    demo.runEmbedded(new PrintWriter(System.out));
  }

  /**
   * Runs {@code CounterDemo} using an embedded {@code Dataset}.
   * @throws StoreException if an embedded {@code DatasetManager} instance cannot be allocated
   */
  public void runEmbedded(Writer out) throws StoreException {

    // tag::embeddedDatasetManager[]
    DatasetManager datasetManager =
        DatasetManager.embedded().offheap("offheap", 128L, MemoryUnit.MB).build(); // <1>
    DatasetConfiguration configuration = datasetManager.datasetConfiguration().offheap("offheap").build(); // <2>
    // end::embeddedDatasetManager[]

    this.counterDemo(datasetManager, configuration, ((out instanceof PrintWriter) ? (PrintWriter)out : new PrintWriter(out)));
  }

  /**
   * Runs {@code CounterDemo} using a clustered {@code Dataset}.
   * @param connectionURI the {@code URI} identifying the cluster server
   * @param offHeapResource the name of the server-side off-heap resource pool to use
   * @throws StoreException if a clustered {@code DatasetManager} instance cannot be allocated
   */
  public void runClustered(URI connectionURI, String offHeapResource, Writer out) throws StoreException {

    // tag::clusterDatasetManager[]
    DatasetManager datasetManager = DatasetManager.clustered(connectionURI).build(); // <1>
    DatasetConfiguration configuration =
        datasetManager.datasetConfiguration().offheap(offHeapResource).build(); // <2>
    // end::clusterDatasetManager[]

    this.counterDemo(datasetManager, configuration, ((out instanceof PrintWriter) ? (PrintWriter)out : new PrintWriter(out)));
  }

  /**
   * Runs a TC Store API demonstration with {@link Dataset} created from the
   * {@link DatasetManager} instance supplied.  Because the {@code DatasetManager} instance
   * determines if the dataset manager backing the dataset is clustered or embedded, this
   * example demonstrates code running unchanged between a clustered and a embedded
   * TC Store deployment.
   *
   * @param datasetManager the {@code DatasetManager} instance from which the {@code Dataset} is created
   * @param configuration the {@code DatasetConfiguration} to use for the {@code Dataset}
   *
   * @throws StoreException on a {@code DatasetManager} fault
   */
  private void counterDemo(DatasetManager datasetManager, DatasetConfiguration configuration, PrintWriter out) throws StoreException {

    // tag::dataSetCreation[]
    datasetManager.newDataset("counters", Type.STRING, configuration); // <3>
    // end::dataSetCreation[]
    try (Dataset<String> counterSet = datasetManager.getDataset("counters", Type.STRING)) {

      // tag::dataSetListing[]
      Map<String, Type<?>> datasets = datasetManager.listDatasets();
      out.println("Existing datasets : " + datasets);
      // end::dataSetListing[]

      // tag::basicCRUD[]
      DatasetWriterReader<String> counterAccess =
          counterSet.writerReader();     // <1>

      String someCounterKey = "someCounter";
      boolean added = counterAccess.add(    // <2>
          someCounterKey, counterCell.newCell(0L), stoppedCell.newCell(false));

      if (added) {
        out.println("No record with the key: " + someCounterKey
            + " existed. The new one was added");
      }

      Optional<Record<String>> someCounterRec = counterAccess.get(someCounterKey); // <3>

      Long longCounterVal = someCounterRec.flatMap(r -> r.get(counterCell)).orElse(0L);   // <4>
      out.println("someCounter is now: " + longCounterVal);

      counterAccess.update(someCounterKey, write(counterCell).value(10L)); // <5>
      someCounterRec = counterAccess.get(someCounterKey);
      out.println("someCounter is now: "
          + someCounterRec.flatMap(r -> r.get(counterCell)).orElse(0L));

      Optional<Record<String>> deletedRecord = counterAccess.on(someCounterKey).delete(); // <6>
      out.println("Deleted record with key: "
          + deletedRecord.map(Record::getKey).orElse("<none>"));
      // end::basicCRUD[]

      for (int i=0; i<10; i++) {
        counterAccess.add("counter" + i,
            counterCell.newCell((long) i), stoppedCell.newCell(false));
      }

      // tag::advancedMutation[]
      String advancedCounterKey = "counter9";
      Optional<String> token = counterAccess
          .on(advancedCounterKey)   // <1>
          .update(UpdateOperation.custom(   // <2>
              record -> {   // <3>
                if (!record.get(stoppedCell).orElse(false)    // <4>
                    && record.get(counterCell).orElse(0L) > 5) {
                  CellSet newCells = new CellSet(record); // <5>
                  newCells.set(stoppedCell.newCell(true));
                  newCells.set(stoppedByCell.newCell("Albin"));
                  newCells.remove(counterCell);
                  return newCells;
                } else {
                  return record;    // <6>
                }
              }))
          .map(Tuple::getSecond)   // <7>
          .map(r -> r.get(stoppedCell).orElse(false)
              ? r.get(stoppedByCell).orElse("<unknown>") : "<not_stopped>");
      // end::advancedMutation[]
      token.ifPresent(name -> out.println("'" + advancedCounterKey + "' was stopped by: " + name));

      // tag::conditionalDelete[]
      deletedRecord = counterAccess.on("counter0").iff(stoppedCell.isFalse()).delete(); // <8>
      // end::conditionalDelete[]

      deletedRecord.ifPresent(r -> out.println("Deleted record with key: '" + r.getKey() + "'"));

      // tag::simpleStreamOperation[]
      OptionalDouble avg;
      try (Stream<Record<String>> recordStream = counterAccess.records()) { // <1>
        avg = recordStream
            .filter(record -> !record.get(stoppedCell).orElse(false)) // <2>
            .mapToLong(record -> record.get(counterCell).orElse(0L)) // <3>
            .average(); // <4>
      }
      // end::simpleStreamOperation[]
      avg.ifPresent(val -> out.println("The average of all the counters that are not stopped yet is: " + val));

      // tag::dslStreamOperation[]
      try (Stream<Record<String>> recordStream = counterAccess.records()) { // <1>
        avg = recordStream
            .filter(stoppedCell.isFalse()) // <2>
            .mapToLong(counterCell.longValueOr(0L)) // <3>
            .average(); // <4>
      }
      // end::dslStreamOperation[]
      avg.ifPresent(val -> out.println("The average calculated using the DSL is: " + val));

      // tag::dslMutation[]
      String dslCounterKey = "counter8";
      token = counterAccess
          .on(dslCounterKey)
          .iff(stoppedCell.isFalse()
              .and(counterCell.valueOr(0L).isGreaterThan(5L)))    // <2>
          .update(
              allOf(write(stoppedCell).value(true),     // <3>
                  write(stoppedByCell).value("Albin"),
                  remove(counterCell)))
          .map(Tuple.second())
          .map(r -> r.get(stoppedCell).orElse(false)
              ? r.get(stoppedByCell).orElse("<unknown>") : "<not stopped>"); // <4>
      // end::dslMutation[]
      token.ifPresent(name -> out.println("DSL mutation: '" + dslCounterKey + "' was stopped by: " + name));

      // tag::runtimeIndexDefinition[]
      Indexing indexing = counterSet.getIndexing();   // <1>
      Operation<Index<Boolean>> indexOp =
          indexing.createIndex(stoppedCell, IndexSettings.btree());   // <2>
      try {
        indexOp.get();  // <3>
      } catch (InterruptedException | ExecutionException e) {
        throw new AssertionError(e);
      }
      // end::runtimeIndexDefinition[]

      // tag::bulkMutation[]
      try (MutableRecordStream<String> recordStream = counterAccess.records()) { // <1>
        recordStream
            .filter(stoppedCell.isFalse())    // <2>
            .mutate(allOf(write(stoppedCell).value(true),   // <3>
                write(stoppedByCell).value("Albin"),
                remove(counterCell)));
      }
      // end::bulkMutation[]
      try (Stream<Record<String>> recordStream = counterAccess.records()) {
        recordStream.forEach(r -> out.format("[postStopping] %s%n", r));
      }

      // tag::asyncOperations[]
      AsyncDatasetWriterReader<String> asyncAccess = counterAccess.async(); // <1>
      Operation<Boolean> addOp = asyncAccess.add("counter10", counterCell.newCell(10L)); // <2>
      Operation<Optional<Record<String>>> getOp =
          addOp.thenCompose((b) -> asyncAccess.get("counter10")); // <3>
      Operation<Void> acceptOp = getOp.thenAccept(or -> or.ifPresent( // <4>
          r -> out.println("The record with key " + r.getKey() + " was added")));

      try {
        acceptOp.get();   // <5>
      } catch (InterruptedException | ExecutionException e) {
        throw new AssertionError(e);
      }
      // end::asyncOperations[]
    }

    // tag::dataSetDestruction[]
    datasetManager.destroyDataset("counters");
    // end::dataSetDestruction[]
  }
}
