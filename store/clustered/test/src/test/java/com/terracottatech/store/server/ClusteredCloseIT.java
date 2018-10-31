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
package com.terracottatech.store.server;

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;
import com.terracottatech.store.indexing.Indexing;
import com.terracottatech.store.stream.RecordStream;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.MultipleFailureException;
import org.terracotta.exception.ConnectionClosedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;


@RunWith(Parameterized.class)
public class ClusteredCloseIT extends PassthroughTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Parameterized.Parameters(name = "Closing {1}")
  public static Object[] data() {
    return new Object[][]{
            {(Consumer<ClusteredCloseIT>) it -> {}, "nothing",
                    Collections.emptyList()},
            {(Consumer<ClusteredCloseIT>) it -> it.datasetManager.close(), "dataset manager",
                    Arrays.asList(MultipleFailureException.class, ConnectionClosedException.class)},
            {(Consumer<ClusteredCloseIT>) it -> it.dataset.close(), "dataset",
                    Arrays.asList(StoreRuntimeException.class, IllegalStateException.class)}
    };
  }

  private final Consumer<ClusteredCloseIT> closer;
  private final List<Class<Exception>> expectedExceptionClasses;

  public ClusteredCloseIT(Consumer<ClusteredCloseIT> closer,
                          @SuppressWarnings("unused") String label,
                          List<Class<Exception>> expectedClasses) {
    this.closer = closer;
    this.expectedExceptionClasses = expectedClasses;
  }

  @SuppressWarnings("unchecked")
  private void closeBeforeUsing() {
    closer.accept(this);
    if (!expectedExceptionClasses.isEmpty()) {
      Matcher<Exception>[] matchers = expectedExceptionClasses.stream()
              .map(Matchers::instanceOf)
              .toArray(Matcher[]::new);
      thrown.expect(anyOf(matchers));
    }
  }

  @Test
  public void testCreateIndex() {
    Indexing indexing = dataset.getIndexing();
    closeBeforeUsing();
    indexing.createIndex(CellDefinition.define("cell", Type.STRING), IndexSettings.BTREE);
  }

  @Test
  public void testStreamCreation() {
    DatasetReader<String> reader = dataset.reader();
    closeBeforeUsing();
    reader.records();
  }

  @Test
  public void testTerminalOperation() {
    RecordStream<String> recordStream = dataset.reader().records();
    closeBeforeUsing();
    @SuppressWarnings("unused")
    long count = recordStream.count();
  }
}
