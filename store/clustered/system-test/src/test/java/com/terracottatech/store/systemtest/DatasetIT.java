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

import com.terracottatech.store.CellSet;
import com.terracottatech.store.Dataset;
import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.Type;

import com.terracottatech.store.manager.DatasetManager;
import org.junit.Test;

import static com.terracottatech.store.Type.BOOL;
import static com.terracottatech.store.Type.CHAR;
import static com.terracottatech.store.Type.DOUBLE;
import static com.terracottatech.store.Type.INT;
import static com.terracottatech.store.Type.LONG;
import static com.terracottatech.store.Type.STRING;
import static com.terracottatech.store.common.test.Employee.CITY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.COUNTRY_ADDRESS;
import static com.terracottatech.store.common.test.Employee.HOUSE_NUMBER_ADDRESS;
import static com.terracottatech.store.common.test.Employee.NAME;
import static com.terracottatech.store.common.test.Employee.SALARY;
import static com.terracottatech.store.common.test.Employee.TELEPHONE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DatasetIT extends CRUDTests {

  private final CellSet cellSet = CellSet.of(NAME.newCell("Mahatma Gandhi"),
      HOUSE_NUMBER_ADDRESS.newCell(100),
      CITY_ADDRESS.newCell("Porbandar"),
      COUNTRY_ADDRESS.newCell("India"),
      SALARY.newCell(1D),
      TELEPHONE.newCell(100L));

  @Test
  public void intKeyTypeDatasetTest() throws Exception {
    keyTypeTest(INT, Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  @Test
  public void longKeyTypeDatasetTest() throws Exception {
    keyTypeTest(LONG, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Test
  public void doubleKeyTypeDatasetTest() throws Exception {
    keyTypeTest(DOUBLE, Double.MIN_VALUE, Double.MAX_VALUE);
  }

  @Test
  public void boolKeyTypeDatasetTest() throws Exception {
    keyTypeTest(BOOL, true, false);
  }

  @Test
  public void charKeyTypeDatasetTest() throws Exception {
    keyTypeTest(CHAR, '\000', 'A', '\u05D0');
  }

  @Test
  public void stringKeyTypeDatasetTest() throws Exception {
    keyTypeTest(STRING, "asdf", " ", "1234", "\\&.@**\\", "");
  }

  @SafeVarargs
  private final <K extends Comparable<K>> void keyTypeTest(Type<K> keyType, K... keys) throws Exception {
    Dataset<K> keyTypedataset;
    DatasetWriterReader<K> keyTypeDatasetWriterReader;
    String datasetName = getClass().getSimpleName() + "#" + testName.getMethodName();

    DatasetManager datasetManager = datasetManagerType.getDatasetManager();
    assertTrue(datasetManager.newDataset(datasetName, keyType, datasetManagerType.getDatasetConfiguration()));
    assertNotNull(keyTypedataset = datasetManager.getDataset(datasetName, keyType));
    assertNotNull(keyTypeDatasetWriterReader = keyTypedataset.writerReader());

    for (K key : keys) {
      assertTrue(keyTypeDatasetWriterReader.add(key, cellSet));
      assertThat(new CellSet(keyTypeDatasetWriterReader.get(key).orElseThrow(() -> new AssertionError("key=" + key))),
          equalTo(cellSet));
    }

    keyTypedataset.close();
  }
}
