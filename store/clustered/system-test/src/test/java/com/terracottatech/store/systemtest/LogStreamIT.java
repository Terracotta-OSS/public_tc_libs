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

package com.terracottatech.store.systemtest;

import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringWriter;

import static com.terracottatech.store.function.BuildableFunction.identity;
import static com.terracottatech.store.stream.RecordStream.log;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SSN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

public class LogStreamIT extends CRUDTests {

  private static final Logger STREAM_LOGGER = Logger.getLogger("StreamLogger");
  private static final Level LOGGER_LEVEL = STREAM_LOGGER.getLevel();

  @BeforeClass
  public static void configureLogger() {
    STREAM_LOGGER.setLevel(Level.ALL);
  }

  @AfterClass
  public static void deconfigureLogger() {
    STREAM_LOGGER.setLevel(LOGGER_LEVEL);
  }

  private WriterAppender writerAppender;
  private StringWriter stringWriter;

  @Before
  public void setUp() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      // Read Employee.csv and initialize List of Employee i.e. employeeList
      initializeEmployeeList();

      // Fill in data to employee Dataset using the employeeList
      employeeList.stream()
          .forEach(employee -> employeeWriterReader.add(employee.getEmpID(), employee.getCellSet()));

      // Setup WriterAppender
      stringWriter = new StringWriter();
      PatternLayout patternLayout = new PatternLayout("%m%n");
      writerAppender = new WriterAppender(patternLayout, stringWriter);
      writerAppender.setEncoding("UTF-8");
      writerAppender.setThreshold(Level.ALL);
      // Add it to logger
      STREAM_LOGGER.addAppender(writerAppender);
    }
  }

  @After
  public void tearDown() {
    STREAM_LOGGER.removeAppender(writerAppender);
    writerAppender.close();
  }

  @Test
  public void portableStreamPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      IntCellDefinition ROLL = CellDefinition.defineInt("roll");
      StringBuilder stringBuilder = new StringBuilder();

      // Stream and logging operations portable
      employeeReader.records()
          .peek(log("SSN : {}, ROLL : {}, NAME : {}",
              SSN.valueOr(100),
              ROLL.valueOr(100),
              NAME.valueOr("Rahul")))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(", ROLL : ")
              .append(rec.get(ROLL).orElse(100))
              .append(", NAME : ")
              .append(rec.get(NAME).orElse("Rahul"))
              .append(System.getProperty("line.separator")));

      if (datasetManagerType.isEmbedded()) {
        assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
      } else {
        assertTrue(stringWriter.toString().isEmpty());
      }
    }
  }

  @Test
  public void portableStreamNonPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeReader.records()
          .peek(log("SSN : {}", (Record<?> rec) -> rec.get(SSN).orElse(100)))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void nonPortableStreamPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeReader.records()
          .map(rec -> rec)
          .peek(log("SSN : {}", SSN.valueOr(100)))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void nonPortableStreamNonPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeReader.records()
          .map(rec -> rec)
          .peek(log("SSN : {}, NAME : {}", (Record<?> rec) -> rec.get(SSN).orElse(100), NAME.valueOr("Rahul")))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(", NAME : ")
              .append(rec.get(NAME).orElse("Rahul"))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void portableStreamIdentityLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeReader.records()
          .peek(log("{}", identity()))
          .peek(rec -> stringBuilder.append(rec.toString()).append(System.getProperty("line.separator")))
          .forEach(t -> {
          });

      if (datasetManagerType.isEmbedded()) {
        assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
      } else {
        assertTrue(stringWriter.toString().isEmpty());
      }
    }
  }

  @Test
  public void nonPortableStreamIdentityLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeReader.records()
          .map(rec -> rec)
          .peek(log("{}", identity()))
          .peek(rec -> stringBuilder.append(rec.toString()).append(System.getProperty("line.separator")))
          .map(Record.keyFunction())
          .peek(log("Key : {}", identity()))
          .peek(key -> stringBuilder.append("Key : ").append(key.toString()).append(System.getProperty("line.separator")))
          .forEach(t -> {
          });

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void portableMutativeStreamPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      IntCellDefinition ROLL = CellDefinition.defineInt("roll");
      StringBuilder stringBuilder = new StringBuilder();

      // Stream and logging operations portable
      employeeWriterReader.records()
          .peek(log("SSN : {}, ROLL : {}, NAME : {}",
              SSN.valueOr(100),
              ROLL.valueOr(100),
              NAME.valueOr("Rahul")))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(", ROLL : ")
              .append(rec.get(ROLL).orElse(100))
              .append(", NAME : ")
              .append(rec.get(NAME).orElse("Rahul"))
              .append(System.getProperty("line.separator")));

      if (datasetManagerType.isEmbedded()) {
        assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
      } else {
        assertTrue(stringWriter.toString().isEmpty());
      }
    }
  }

  @Test
  public void portableMutativeStreamNonPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeWriterReader.records()
          .peek(log("SSN : {}", (Record<?> rec) -> rec.get(SSN).orElse(100)))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void nonPortableMutativeStreamPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeWriterReader.records()
          .map(rec -> rec)
          .peek(log("SSN : {}", SSN.valueOr(100)))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void nonPortableMutativeStreamNonPortableLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeWriterReader.records()
          .map(rec -> rec)
          .peek(log("SSN : {}, NAME : {}", (Record<?> rec) -> rec.get(SSN).orElse(100), NAME.valueOr("Rahul")))
          .forEach(rec -> stringBuilder.append("SSN : ")
              .append(rec.get(SSN).orElse(100))
              .append(", NAME : ")
              .append(rec.get(NAME).orElse("Rahul"))
              .append(System.getProperty("line.separator")));

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }

  @Test
  public void portableMutativeStreamIdentityLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeWriterReader.records()
          .peek(log("{}", identity()))
          .peek(rec -> stringBuilder.append(rec.toString()).append(System.getProperty("line.separator")))
          .forEach(t -> {
          });

      if (datasetManagerType.isEmbedded()) {
        assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
      } else {
        assertTrue(stringWriter.toString().isEmpty());
      }
    }
  }

  @Test
  public void nonPortableMutativeStreamIdentityLogger() {
    try (Dataset<Integer> dataset = getTestDataset()) {
      DatasetWriterReader<Integer> employeeWriterReader = dataset.writerReader();
      DatasetReader<Integer> employeeReader = dataset.reader();
      StringBuilder stringBuilder = new StringBuilder();

      employeeWriterReader.records()
          .map(rec -> rec)
          .peek(log("{}", identity()))
          .peek(rec -> stringBuilder.append(rec.toString()).append(System.getProperty("line.separator")))
          .map(Record.keyFunction())
          .peek(log("Key : {}", identity()))
          .peek(key -> stringBuilder.append("Key : ").append(key.toString()).append(System.getProperty("line.separator")))
          .forEach(t -> {
          });

      assertThat(stringBuilder.toString(), equalTo(stringWriter.toString()));
    }
  }
}
