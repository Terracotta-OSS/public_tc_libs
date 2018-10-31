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

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import org.junit.Test;

import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatasetReferenceTest {
  private final Supplier<Boolean> closed = () -> false;

  @Test
  public void createRetrieveCloseDestroy() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    mockForDatasetCreation(datasetFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());
    reference.create("dataset", configuration);
    Dataset<?> dataset = reference.retrieve();
    dataset.close();

    reference.destroy();
  }

  @Test(expected = StoreException.class)
  public void noCloseMeansError() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    mockForDatasetCreation(datasetFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());
    reference.create("dataset", configuration);
    reference.retrieve();

    reference.destroy();
  }

  @Test
  public void retrieveAs() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    mockForDatasetCreation(datasetFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());
    reference.create("dataset", configuration);
    Dataset<?> dataset = reference.retrieveAs("dataset", Type.LONG);
    dataset.close();

    reference.destroy();
  }

  @Test(expected = StoreException.class)
  public void retrieveAsChecksType() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    mockForDatasetCreation(datasetFactory);

    EmbeddedDatasetConfiguration configuration = new EmbeddedDatasetConfiguration("offheap", "disk", emptyMap());
    reference.create("dataset", configuration);
    Dataset<?> dataset = reference.retrieveAs("dataset", Type.STRING);
    dataset.close();

    reference.destroy();
  }

  @Test
  public void setValue() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    setDataset(reference);

    Dataset<?> dataset = reference.retrieve();
    dataset.close();

    reference.destroy();
  }

  @Test(expected = StoreException.class)
  public void noCloseMeansErrorForSet() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    setDataset(reference);

    reference.retrieve();

    reference.destroy();
  }

  @Test
  public void retrieveAsForSet() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    setDataset(reference);

    Dataset<?> dataset = reference.retrieveAs("dataset", Type.LONG);
    dataset.close();

    reference.destroy();
  }

  @Test(expected = StoreException.class)
  public void retrieveAsChecksTypeForSet() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    DatasetReference<Long> reference = new DatasetReference<>(Type.LONG, datasetFactory, closed);
    setDataset(reference);

    Dataset<?> dataset = reference.retrieveAs("dataset", Type.STRING);
    dataset.close();

    reference.destroy();
  }

  @SuppressWarnings("unchecked")
  private void mockForDatasetCreation(DatasetFactory datasetFactory) throws Exception {
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create(eq("dataset"), eq(Type.LONG), any(EmbeddedDatasetConfiguration.class))).thenReturn(sovereignDataset);
  }

  @SuppressWarnings("unchecked")
  private void setDataset(DatasetReference<Long> reference) {
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    reference.setValue(sovereignDataset);
  }
}
