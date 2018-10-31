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

import com.terracottatech.store.Cell;
import com.terracottatech.store.ConditionalReadWriteRecordAccessor;
import com.terracottatech.store.ReadWriteRecordAccessor;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.configuration.MemoryUnit;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * A simple CRUD example using pre-defined {@link CellDefinition} instances.
 */
// The formatting in this class is influenced by the inclusion of fragments from this class in documentation
public class CrudDemo {

  public static void main(String[] args) throws StoreException {
    //tag::creationIndexDefinition[]
    DatasetManager datasetManager =
        DatasetManager.embedded().offheap("offheap", 64, MemoryUnit.MB).build();
    DatasetConfiguration configuration = datasetManager.datasetConfiguration() // <1>
        .offheap("offheap")
        .index(Person.LAST_NAME, IndexSettings.btree()) // <2>
        .index(Person.NICENESS, IndexSettings.btree()) // <3>
        .build();
    datasetManager.newDataset("people", Type.STRING, configuration); // <4>
    //end::creationIndexDefinition[]

    // define, create/retrieve a dataset with String keys
    try (Dataset<String> persons = datasetManager.getDataset("people", Type.STRING)) {

      // tag::writerReader[]
      DatasetWriterReader<String> writerReader = persons.writerReader(); // <1>
      // end::writerReader[]

      // tag::addingRecords[]
      String person1 = "p1";
      writerReader.add(person1, // <1>
          Person.FIRST_NAME.newCell("Marcus"), // <2>
          Person.LAST_NAME.newCell("Aurelius"),
          Person.RATED.newCell(true),
          Person.NICENESS.newCell(0.65D));
      // end::addingRecords[]
      String person2 = "p2";
      writerReader.add(person2,
          Person.FIRST_NAME.newCell("Genghis"),
          Person.LAST_NAME.newCell("Khan"),
          Person.RATED.newCell(true),
          Person.NICENESS.newCell(0.18D));
      String person3 = "p3";
      writerReader.add(person3,
          Person.FIRST_NAME.newCell("George"),
          Person.LAST_NAME.newCell("MacDonald"),
          Person.RATED.newCell(false));

      try (final Stream<Record<String>> recordStream = writerReader.records()) {
        System.out.println("Total number of people is now: " + recordStream.count());
      }

      // tag::readingRecords[]
      Record<String> marcus = writerReader.get(person1).orElseThrow(AssertionError::new); // <1>
      // end::readingRecords[]

      // examine read on
      for (Cell<?> cell : marcus) {
        CellDefinition<?> def = cell.definition();
        System.out.println("Found cell: " + def.name() + " of type: " + def.type() + " has value: " + cell.value());
      }

      // find the count of People that have a rating
      try (Stream<Record<String>> recordStream = writerReader.records()) {
        recordStream.filter(Person.RATED.isTrue()).count();
      }

      // tag::updatingRecords[]
      writerReader.update(marcus.getKey(), UpdateOperation.write(Person.NICENESS).value(0.85D)); // <1>
      writerReader.update(person2, UpdateOperation.allOf(
          UpdateOperation.write(Person.RATED).value(false), UpdateOperation.remove(Person.NICENESS))); // <2>
      writerReader.update(person3, UpdateOperation.allOf(
          UpdateOperation.write(Person.RATED).value(true), UpdateOperation.write(Person.NICENESS).value(0.92D)));
      // end::updatingRecords[]

      // find records (find persons that are mostly nice)
      try (Stream<Record<String>> recordStream = writerReader.records()) {
        recordStream
            .filter(Person.NICENESS.exists().and(Person.NICENESS.value().isGreaterThan(0.5D)))
            .forEach(person -> System.out.println("Found a nice person: " + person.get(Person.LAST_NAME)));
      }

      // tag::deletingRecords[]
      writerReader.delete(marcus.getKey()); // <1>
      // end::deletingRecords[]

      try (Stream<Record<String>> recordStream = writerReader.records()) {
        System.out.println("Total number of people is now: " + recordStream.count());
      }

      // tag::accessors[]
      ReadWriteRecordAccessor<String> recordAccessor = writerReader.on(person3); // <1>

      recordAccessor.read(record -> record.get(Person.NICENESS).get()); // <2>

      recordAccessor.upsert(Person.BIRTH_YEAR.newCell(2000), Person.PICTURE.newCell(new byte[1024])); // <3>

      Optional<Integer> ageDiff = recordAccessor.update(UpdateOperation.write(Person.BIRTH_YEAR.newCell(1985)), (record1, record2) ->
              record1.get(Person.BIRTH_YEAR).get() - record2.get(Person.BIRTH_YEAR).get()); // <4>

      ConditionalReadWriteRecordAccessor<String> conditionalReadWriteRecordAccessor =
              recordAccessor.iff(record -> record.get(Person.BIRTH_YEAR).get().equals(1985)); // <5>

      Record<String> record = conditionalReadWriteRecordAccessor.read().get(); // <6>
      conditionalReadWriteRecordAccessor.update(UpdateOperation.write(Person.RATED).value(false)); // <7>
      conditionalReadWriteRecordAccessor.delete(); // <8>
      // end::accessors[]

      System.out.println("The Age difference is " + ageDiff.get());

      try (Stream<Record<String>> recordStream = writerReader.records()) {
        System.out.println("Total number of people is now: " + recordStream.count());
      }
    }
  }
}
