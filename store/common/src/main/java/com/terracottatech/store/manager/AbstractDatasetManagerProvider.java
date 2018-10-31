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

import com.terracottatech.store.DatasetKeyTypeMismatchException;
import com.terracottatech.store.DatasetMissingException;
import com.terracottatech.store.StoreException;
import com.terracottatech.store.StoreRuntimeException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

abstract class AbstractDatasetManagerProvider implements DatasetManagerProvider {
  void ensureDatasets(DatasetManager datasetManager,
                      DatasetManagerConfiguration datasetManagerConfiguration,
                      ConfigurationMode configurationMode) throws StoreException {

    Set<String> createdDatasets = new HashSet<>();
    for (Map.Entry<String, DatasetManagerConfiguration.DatasetInfo<?>> entry : datasetManagerConfiguration.getDatasets().entrySet()) {
      String datasetName = entry.getKey();
      DatasetManagerConfiguration.DatasetInfo<?> datasetInfo = entry.getValue();

      switch (configurationMode) {
        case AUTO: {
          try {
            datasetManager.newDataset(datasetName, datasetInfo.getType(), datasetInfo.getDatasetConfiguration());
          } catch (DatasetKeyTypeMismatchException e) {
            throw new StoreRuntimeException("Validation failed for dataset with name " + datasetName + ", " +
                                            "configuration mode: " + configurationMode, e);
          }
          break;
        }
        case CREATE: {
          String errMsg = "A dataset with the name " + datasetName + " already exists, configuration mode: " +
                          configurationMode +
                          (!createdDatasets.isEmpty() ? "; following datasets were created so far: " +
                                                          createdDatasets : "");
          try {
            boolean created = datasetManager.newDataset(datasetName, datasetInfo.getType(), datasetInfo.getDatasetConfiguration());
            if (!created) {
              throw new StoreRuntimeException(errMsg);
            }
            createdDatasets.add(datasetName);
          } catch (DatasetKeyTypeMismatchException e) {
            throw new StoreRuntimeException(errMsg, e);
          }
          break;
        }
        case VALIDATE: {
          try {
            datasetManager.getDataset(datasetName, datasetInfo.getType()).close();
          } catch (DatasetMissingException e) {
            throw new StoreRuntimeException("Dataset with name " + datasetName + " is missing, configuration mode: "
                                            + configurationMode, e);
          } catch (DatasetKeyTypeMismatchException e) {
            throw new StoreRuntimeException("Validation failed for dataset with name " + datasetName + ", " +
                                            "configuration mode: " + configurationMode, e);
          }
        }
      }
    }
  }
}
