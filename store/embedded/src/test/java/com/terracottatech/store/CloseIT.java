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
package com.terracottatech.store;

import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.MemoryUnit;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.manager.DatasetManager;
import com.terracottatech.store.stream.RecordStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.function.Consumer;

import static com.terracottatech.store.manager.DatasetManager.embedded;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;


@RunWith(Parameterized.class)
public class CloseIT {

  private DatasetManager datasetManager;
  private Dataset<Long> dataset;

  @Before
  public void setUp() throws StoreException {
    datasetManager = embedded()
            .offheap("offheap", 10, MemoryUnit.MB)
            .build();
    DatasetConfiguration configuration = datasetManager.datasetConfiguration()
            .offheap("offheap")
            .build();
    boolean created = datasetManager.newDataset("test", Type.LONG, configuration);
    assertThat(created, is(true));
    dataset = datasetManager.getDataset("test", Type.LONG);
  }

  @After
  public void tearDown() {
    if (dataset != null) {
      dataset.close();
    }
    if (datasetManager != null) {
      datasetManager.close();
    }
  }

  @Parameterized.Parameters(name = "Closing {1}")
  public static Object[] data() {
    return new Object[][]{
            {(Consumer<CloseIT>) it -> it.datasetManager.close(), "dataset manager", IllegalStateException.class},
            {(Consumer<CloseIT>) it -> it.dataset.close(), "dataset", null}
    };
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final Consumer<CloseIT> closer;
  private final Class<? extends Exception> optionalExceptionClass;

  public CloseIT(Consumer<CloseIT> closer, @SuppressWarnings("unused") String label, Class<? extends Exception> exceptionClass) {
    this.closer = closer;
    this.optionalExceptionClass = exceptionClass;
  }

  private void closeBeforeAndExpectStandardException() {
    closer.accept(this);
    thrown.expect(StoreRuntimeException.class);
  }

  private void closeBeforeAndExpectOptionalException() {
    closer.accept(this);
    if (optionalExceptionClass != null) {
      thrown.expect(optionalExceptionClass);
    }
  }

  @Test
  public void testGetIndexing() {
    closeBeforeAndExpectStandardException();
    dataset.getIndexing();
  }

  @Test
  public void testCreateIndex() {
    Indexing indexing = dataset.getIndexing();
    closeBeforeAndExpectStandardException();
    indexing.createIndex(CellDefinition.define("cell", Type.STRING), IndexSettings.BTREE);
  }

  @Test
  public void testReaderAccess() {
    closeBeforeAndExpectStandardException();
    dataset.reader();
  }

  @Test
  public void testStreamCreation() {
    DatasetReader<Long> reader = dataset.reader();
    closeBeforeAndExpectOptionalException();
    reader.records();
  }

  @Test
  public void testTerminalOperation() {
    RecordStream<Long> recordStream = dataset.reader().records();
    closeBeforeAndExpectOptionalException();
    @SuppressWarnings("unused")
    long count = recordStream.count();
  }
}
