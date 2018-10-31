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
import com.terracottatech.store.transactions.api.TransactionController.ReadOnlyExecutionBuilder;
import com.terracottatech.store.transactions.api.TransactionController.ReadWriteExecutionBuilder;
import com.terracottatech.store.transactions.api.TransactionalAction;
import com.terracottatech.store.transactions.api.TransactionalTask;

import java.util.concurrent.TimeUnit;

public class TransactionControllerDemo {
  public static final DoubleCellDefinition SALARY = CellDefinition.defineDouble("salary");
  public static final DoubleCellDefinition TURNOVER = CellDefinition.defineDouble("turnover");


  @SuppressWarnings("unchecked")
  public static void main(String args[]) throws Exception {
    try (DatasetManager datasetManager = DatasetManager.embedded()
      .offheap("offheap", 128L, MemoryUnit.MB)
      .build()) {

      DatasetConfigurationBuilder datasetConfigurationBuilder = datasetManager.datasetConfiguration().offheap("offheap");
      datasetManager.newDataset("employee", Type.INT, datasetConfigurationBuilder);
      datasetManager.newDataset("customer", Type.INT, datasetConfigurationBuilder);

      Dataset<Integer> employeeDataset = datasetManager.getDataset("employee", Type.INT);
      Dataset<Integer> customerDataset = datasetManager.getDataset("customer", Type.INT);

      DatasetReader<Integer> employeeReader = employeeDataset.reader();
      DatasetReader<Integer> customerReader = customerDataset.reader();
      DatasetWriterReader<Integer> employeeWriterReader = employeeDataset.writerReader();
      DatasetWriterReader<Integer> customerWriterReader = customerDataset.writerReader();

      employeeWriterReader.add(1, SALARY.newCell(100D));
      customerWriterReader.add(100, TURNOVER.newCell(200D));

      // TransactionController creation that also would create the SystemTransactions internally
      // tag::TransactionControllerCreation[]
      TransactionController transactionController = TransactionController.createTransactionController // <1>
        (datasetManager, // <2>
          datasetConfigurationBuilder); // <3>
      // end::TransactionControllerCreation[]

      // TransactionController creation with SystemTransactions already created
      // tag::TransactionControllerCreationWithExistingSystemTransactions[]
      TransactionController controller1 = TransactionController.createTransactionController(datasetManager); // <1>
      // end::TransactionControllerCreationWithExistingSystemTransactions[]

      // TransactionController creation using a new default transaction timeout
      // tag::TransactionControllerCreationWithTransactionTimeout[]
      TransactionController controller2 = transactionController.withDefaultTimeOut(50, TimeUnit.SECONDS); // <1>
      // end::TransactionControllerCreationWithTransactionTimeout[]

      // tag::ReadOnlyExecutionBuilder[]
      ReadOnlyExecutionBuilder readOnlyExecutionBuilder = controller1.transact() // <1>
        .timeout(100, TimeUnit.SECONDS) // <2>
        .using("empReader", employeeReader)    // <3>
        .using("custReader", customerReader);  // <4>
      // end::ReadOnlyExecutionBuilder[]

      readOnlyExecutionBuilder.execute(readers -> {
        DatasetReader<Integer> empTransactionalReader = (DatasetReader<Integer>) readers.get("empReader");
        DatasetReader<Integer> custTransactionalReader = (DatasetReader<Integer>) readers.get("custReader");

        System.out.println("Total employee salary = " +
          empTransactionalReader.records().mapToDouble(SALARY.doubleValueOr(0D)).sum());

        System.out.println("Total customer turnover = " +
          custTransactionalReader.records().mapToDouble(TURNOVER.doubleValueOr(0D)).sum());

        return null;
      });

      // tag::ReadWriteExecutionBuilder[]
      ReadWriteExecutionBuilder readWriteExecutionBuilder = controller2.transact()
        .using("custWriterReader", customerWriterReader)  // <1>
        .using("empReader", employeeReader); // <2>
      // end::ReadWriteExecutionBuilder[]

      readWriteExecutionBuilder.execute((writerReaders, readers) -> {
        DatasetReader<Integer> empTransactionalReader = (DatasetReader<Integer>) readers.get("empReader");
        DatasetWriterReader<Integer> custTransactionalWriterReader = (DatasetWriterReader<Integer>) writerReaders.get("custWriterReader");

        System.out.println("Total employee salary = " +
          empTransactionalReader.records().mapToDouble(SALARY.doubleValueOr(0D)).sum());

        custTransactionalWriterReader.on(100)
          .update(UpdateOperation.write(TURNOVER).doubleResultOf(TURNOVER.doubleValueOrFail().increment()));
        System.out.println("Total customer turnover = " +
          custTransactionalWriterReader.records().mapToDouble(TURNOVER.doubleValueOr(0D)).sum());

        return null;
      });

      // tag::TransactionControllerExecute[]
      Long numberOfEmployees = transactionController.execute(employeeReader, // <1>
                                  reader -> reader.records().count()); // <2>

      Long numberOfEmployeesAndCustomers = transactionController.execute(employeeReader, customerReader, // <3>
        (empReader, custReader) -> empReader.records().count() + custReader.records().count()); // <4>

      transactionController.execute(employeeWriterReader, // <5>
        (TransactionalTask<DatasetWriterReader<Integer>>) writerReader -> writerReader.on(1).delete()); // <6>
      // end::TransactionControllerExecute[]
    }
  }

}
