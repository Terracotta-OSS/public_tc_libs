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

package com.terracottatech.store.manager;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetFactory;
import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.Type;
import com.terracottatech.store.builder.EmbeddedDatasetConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration.ResourceConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class EmbeddedDatasetManagerTest {

  @Test(expected = DatasetMissingException.class)
  public void getNonExistentDataset() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    datasetManager.getDataset("dataset", Type.LONG);
  }

  @Test
  public void testListDatasets() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    when(datasetFactory.create("datasetLong", Type.LONG, configuration)).thenReturn(mock(SovereignDataset.class));
    when(datasetFactory.create("datasetString", Type.STRING, configuration)).thenReturn(mock(SovereignDataset.class));

    Assert.assertThat(datasetManager.listDatasets().size(), is(0));

    datasetManager.newDataset("datasetLong", Type.LONG, configuration);
    datasetManager.newDataset("datasetString", Type.STRING, configuration);

    Map<String, Type<?>> datasets = datasetManager.listDatasets();
    assertThat(datasets.size(), is(2));
    assertThat(datasets.get("datasetLong"), is(Type.LONG));
    assertThat(datasets.get("datasetString"), is(Type.STRING));

    datasetManager.destroyDataset("datasetString");
    datasets = datasetManager.listDatasets();
    Assert.assertThat(datasets.size(), is(1));
    Assert.assertThat(datasets.get("datasetLong"), is(Type.LONG));

    datasetManager.destroyDataset("datasetLong");
    datasets = datasetManager.listDatasets();
    Assert.assertThat(datasets.size(), is(0));
  }

  @Test
  public void createDataset() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    assertThat(datasetManager.newDataset("dataset", Type.LONG, configuration), is(true));

    Dataset<Long> dataset1 = datasetManager.getDataset("dataset", Type.LONG);
    Dataset<Long> dataset2 = datasetManager.getDataset("dataset", Type.LONG);

    dataset1.close();
    dataset2.close();

    datasetManager.close();

    verify(datasetFactory).checkConfiguration(configuration);
    verify(datasetFactory).create("dataset", Type.LONG, configuration);
    verify(datasetFactory).close();
    verifyNoMoreInteractions(datasetFactory);
  }

  @Test
  public void createDatasetThatAlreadyExists() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    datasetManager.newDataset("dataset", Type.LONG, configuration);
    assertThat(datasetManager.newDataset("dataset", Type.LONG, configuration), is(false));
  }

  @Test
  public void destroyDatasetThatDoesNotExist() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    assertFalse(datasetManager.destroyDataset("dataset"));

    datasetManager.close();
  }

  @Test(expected = StoreException.class)
  public void destroyDatasetInUse() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    datasetManager.newDataset("dataset", Type.LONG, configuration);
    datasetManager.getDataset("dataset", Type.LONG);
    datasetManager.destroyDataset("dataset");
  }

  @Test
  public void destroyMultiplyUsedDataset() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration =
        (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    datasetManager.newDataset("dataset", Type.LONG, configuration);

    Dataset<Long> dataset1 = datasetManager.getDataset("dataset", Type.LONG);
    Dataset<Long> dataset2 = datasetManager.getDataset("dataset", Type.LONG);

    try {
      datasetManager.destroyDataset("dataset");
      fail("Expecting StoreException");
    } catch (StoreException e) {
      // ignored
    }
    verify(datasetFactory, never()).destroy(sovereignDataset);

    dataset1.close();

    try {
      datasetManager.destroyDataset("dataset");
      fail("Expecting StoreException");
    } catch (StoreException e) {
      // ignored
    }
    verify(datasetFactory, never()).destroy(sovereignDataset);

    dataset2.close();

    assertTrue(datasetManager.destroyDataset("dataset"));
    verify(datasetFactory, times(1)).destroy(sovereignDataset);

    assertFalse(datasetManager.destroyDataset("dataset"));
    verify(datasetFactory, times(1)).destroy(sovereignDataset);
  }

  @Test
  public void validDestroy() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    datasetManager.newDataset("dataset", Type.LONG, configuration);
    assertTrue(datasetManager.destroyDataset("dataset"));

    try {
      datasetManager.getDataset("dataset", Type.LONG);
      fail("No exception thrown getting destroyed Dataset");
    } catch (DatasetMissingException e) {
      // Expected exception
    }

    datasetManager.close();
  }

  @Test(expected = DatasetKeyTypeMismatchException.class)
  public void getDatasetWithWrongType() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    EmbeddedDatasetConfiguration configuration = (EmbeddedDatasetConfiguration) datasetManager.datasetConfiguration().offheap("offheap").build();
    SovereignDataset<Long> sovereignDataset = mock(SovereignDataset.class);
    when(datasetFactory.create("dataset", Type.LONG, configuration)).thenReturn(sovereignDataset);

    datasetManager.newDataset("dataset", Type.LONG, configuration);
    datasetManager.getDataset("dataset", Type.LONG);
    datasetManager.getDataset("dataset", Type.STRING);
  }

  @Test
  public void foundExisting() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    SovereignDataset<String> sovereignDataset = mock(SovereignDataset.class);
    when(sovereignDataset.getAlias()).thenReturn("dataset");
    when(sovereignDataset.getType()).thenReturn(Type.STRING);

    datasetManager.foundExistingDataset("disk", sovereignDataset);

    Dataset<String> dataset = datasetManager.getDataset("dataset", Type.STRING);
    dataset.close();

    datasetManager.close();
  }

  @Test(expected = StoreException.class)
  public void foundExistingConflict() throws Exception {
    DatasetFactory datasetFactory = mock(DatasetFactory.class);
    EmbeddedDatasetManager datasetManager = new EmbeddedDatasetManager(datasetFactory, mock(ResourceConfiguration.class));

    SovereignDataset<String> sovereignDataset1 = mock(SovereignDataset.class);
    when(sovereignDataset1.getAlias()).thenReturn("dataset");
    when(sovereignDataset1.getType()).thenReturn(Type.STRING);

    SovereignDataset<String> sovereignDataset2 = mock(SovereignDataset.class);
    when(sovereignDataset2.getAlias()).thenReturn("dataset");
    when(sovereignDataset2.getType()).thenReturn(Type.STRING);

    datasetManager.foundExistingDataset("disk", sovereignDataset1);
    datasetManager.foundExistingDataset("disk", sovereignDataset2);
  }
}
