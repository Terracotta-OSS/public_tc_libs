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

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Tuple;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.RecordStream;

import java.util.Optional;

import static com.terracottatech.store.embedded.demo.Employee.BIRTH_YEAR;
import static com.terracottatech.store.embedded.demo.Employee.COUNTRY;
import static com.terracottatech.store.embedded.demo.Employee.GENDER;
import static com.terracottatech.store.embedded.demo.Employee.NAME;
import static com.terracottatech.store.embedded.demo.Employee.SALARY;

public class RecordStreamDemo {
  public static void main(String[] args) throws StoreException {
    try (DatasetManager datasetManager = DatasetManager.embedded()
        .offheap("offheap", 128L, MemoryUnit.MB)
        .build()) {

      DatasetConfiguration datasetConfiguration = datasetManager.datasetConfiguration().offheap("offheap").build();
      datasetManager.newDataset("employee", Type.INT, datasetConfiguration);

      try (Dataset<Integer> dataset = datasetManager.getDataset("employee", Type.INT)) {
        DatasetWriterReader<Integer> employeeWriterReader =
            dataset.writerReader();
        DatasetReader<Integer> employeeReader = dataset.reader();

        addSampleData(employeeWriterReader);

        // RecordStream creation
        // tag::RecordStreamCreation[]
        long numMaleEmployees = employeeReader.records()   // <1>
            .filter(GENDER.value().is('M'))   // <2>
            .count();   // <3>
        // end::RecordStreamCreation[]

        System.out.println("Number of male employees = " + numMaleEmployees);

        //Examples of batched and inline stream execution
        // tag::RecordStreamBatchedInline[]
        Optional<Record<Integer>> record = employeeReader.records()
          .explain(System.out::println)  // <1>
          .batch(2) // <2>
          .peek(RecordStream.log("{} from {}", NAME.valueOr(""), COUNTRY.valueOr(""))) // <3>
          .filter(COUNTRY.value().is("USA"))
          .findAny();

        long count = employeeReader.records()
            .inline()  // <4>
            .count();
        // end::RecordStreamBatchedInline[]

        System.out.println("Total number of Employee = " + count);

        if (record.isPresent()) {
          System.out.println(record.get().get(NAME).get() + " is based in USA");
        }

        // Examples of Mutate operations on RecordStream
        // tag::mutateThen[]
        double sum = employeeWriterReader.records()    // <1>
          .mutateThen(UpdateOperation.write(SALARY).doubleResultOf(SALARY.doubleValueOrFail().increment())) // <2>
          .map(Tuple::getSecond)    // <3>
          .mapToDouble(SALARY.doubleValueOrFail())
          .sum();
        // end::mutateThen[]

        System.out.println("New Total salary of all the employees = " + sum);

        // tag::mutate[]
        employeeWriterReader.records()
            .filter(GENDER.value().is('M'))
            .mutate(UpdateOperation.write(SALARY).doubleResultOf(SALARY.doubleValueOrFail().decrement())); // <1>
        // end::mutate[]

        System.out.println("New Total salary of all the male employees = "
            + employeeReader.records()
                            .filter(GENDER.value().is('M'))
                            .mapToDouble(SALARY.doubleValueOrFail())
                            .sum());

        printDetailsOfCurrentEmployees(employeeReader);

        // tag::deleteThen[]
        employeeWriterReader.records()
            .filter(BIRTH_YEAR.value().isGreaterThan(1985))
            .deleteThen()   // <1>
            .map(NAME.valueOrFail())   // <2>
            .forEach(name -> System.out.println("Deleted record of " + name));
        // end::deleteThen[]

        // tag::delete[]
        employeeWriterReader.records()
            .filter(BIRTH_YEAR.value().isLessThan(1945))
            .delete(); // <1>
        // end::delete[]

        printDetailsOfCurrentEmployees(employeeReader);
      }
    }
  }

  private static void addSampleData(DatasetWriterReader<Integer> employeeWriterReader) {
    employeeWriterReader.add(1,
        NAME.newCell("Rahul"),
        GENDER.newCell('M'),
        COUNTRY.newCell("India"),
        BIRTH_YEAR.newCell(1940),
        SALARY.newCell(100_000D));

    employeeWriterReader.add(2,
        NAME.newCell("John"),
        GENDER.newCell('M'),
        COUNTRY.newCell("USA"),
        BIRTH_YEAR.newCell(1960),
        SALARY.newCell(125_000D));

    employeeWriterReader.add(3,
        NAME.newCell("Maria"),
        GENDER.newCell('F'),
        COUNTRY.newCell("Italy"),
        BIRTH_YEAR.newCell(1985),
        SALARY.newCell(130_000D));

      employeeWriterReader.add(4,
          NAME.newCell("Monica"),
          GENDER.newCell('F'),
          COUNTRY.newCell("Egypt"),
          BIRTH_YEAR.newCell(1990),
          SALARY.newCell(99_000D));
  }

  private static void printDetailsOfCurrentEmployees(DatasetReader<Integer> employeeReader) {
    StringBuilder stringBuilder = new StringBuilder("(Name, BirthYear) of current employees are");
    employeeReader.records()
        .forEach(rec -> stringBuilder.append(" (" + rec.get(NAME).get() + ", " + rec.get(BIRTH_YEAR).get() + ")"));
    System.out.println(stringBuilder.toString());
  }
}
