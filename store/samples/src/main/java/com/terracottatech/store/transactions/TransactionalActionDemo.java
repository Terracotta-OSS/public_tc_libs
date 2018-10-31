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
package com.terracottatech.store.transactions;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;
import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.transactions.api.TransactionController;

public class TransactionalActionDemo {
  public static final DoubleCellDefinition SALARY = CellDefinition.defineDouble("salary");

  @SuppressWarnings("unchecked")
  public static void main(String args[]) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
      .offheap("offheap", 128L, MemoryUnit.MB)
      .build()) {

      DatasetConfigurationBuilder datasetConfigurationBuilder = datasetManager.datasetConfiguration().offheap("offheap");
      datasetManager.newDataset("employee", Type.INT, datasetConfigurationBuilder);
      Dataset<Integer> employeeDataset = datasetManager.getDataset("employee", Type.INT);

      DatasetReader<Integer> employeeReader = employeeDataset.reader();
      DatasetWriterReader<Integer> employeeWriterReader = employeeDataset.writerReader();

      employeeWriterReader.add(1, SALARY.newCell(100D));
      employeeWriterReader.add(2, SALARY.newCell(200D));

      TransactionController transactionController =
        TransactionController.createTransactionController(datasetManager, datasetConfigurationBuilder);

      // tag::ReadOnlyTransaction[]
      Double totalSalary = transactionController.transact()
        .using("empReader", employeeReader)
        .execute(readers -> { // <1>
          @SuppressWarnings("unchecked")
          DatasetReader<Integer> empTransactionalReader = (DatasetReader<Integer>) readers.get("empReader"); // <2>

          double first100EmployeesSalaryCount = 0;
          for (int i = 1; i < 100; i++) {
            first100EmployeesSalaryCount +=
              empTransactionalReader.get(i).map(rec -> rec.get(SALARY).orElse(0D)).orElse(0D); // <3>
          }

          return first100EmployeesSalaryCount; // <4>
        });
      // end::ReadOnlyTransaction[]

      System.out.println("Total Salary = " + totalSalary);

      // tag::ReadWriteTransaction[]
      int numberOfRecordsUpdated = transactionController.transact()
        .using("empReader", employeeReader)
        .using("empWriterReader", employeeWriterReader)
        .execute((writerReaders, readers) -> { // <1>
          DatasetWriterReader<Integer> empTransactionalWriterReader =
            (DatasetWriterReader<Integer>) writerReaders.get("empWriterReader"); // <2>
          DatasetReader<Integer> empTransactionalReader =
            (DatasetReader<Integer>) readers.get("empReader"); // <2>

          int numRecordsUpdated = 0;
          for (int i = 1; i < 100; i++) {
            numRecordsUpdated += empTransactionalWriterReader.on(i) // <3>
              .update(UpdateOperation.write(SALARY).doubleResultOf(SALARY.doubleValueOr(0D).add(100D)))
              .isPresent() ? 1 : 0;
          }

          System.out.println("Total Employee = " + empTransactionalReader.records().count()); // <3>
          return numRecordsUpdated; // <4>
        });
      // end::ReadWriteTransaction[]

      System.out.println("Number of records updated = " + numberOfRecordsUpdated);
    }
  }
}
