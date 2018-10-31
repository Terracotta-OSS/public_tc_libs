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
package com.terracottatech.store.server.management;

import com.tc.classloader.CommonComponent;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.store.common.DatasetEntityConfiguration;
import com.terracottatech.store.server.storage.factory.PersistentStorageFactory;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.AliasBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;

@Named("SovereignDatasetSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@CommonComponent
public class DatasetSettingsManagementProvider extends AliasBindingManagementProvider<DatasetBinding> {

  public DatasetSettingsManagementProvider() {
    super(DatasetBinding.class);
  }

  @Override
  protected ExposedDatasetBinding internalWrap(Context context, DatasetBinding managedObject) {
    return new ExposedDatasetBinding(context, managedObject);
  }

  static class ExposedDatasetBinding extends ExposedAliasBinding<DatasetBinding> {

    ExposedDatasetBinding(Context context, DatasetBinding binding) {
      super(context.with("type", "SovereignDataset"), binding);
    }

    @Override
    public Collection<? extends Settings> getDescriptors() {
      DatasetEntityConfiguration<?> configuration = getBinding().getValue();

      String[] configuredIndexes = configuration.getDatasetConfiguration().getIndexes().entrySet().stream().map(cellDefinitionIndexSettingsEntry -> {
        String cellName = cellDefinitionIndexSettingsEntry.getKey().name();
        String cellType = cellDefinitionIndexSettingsEntry.getKey().type().getJDKType().getSimpleName();
        String indexSetting = cellDefinitionIndexSettingsEntry.getValue().toString();
        return cellName + "$$$" + cellType + "$$$" + indexSetting;
      }).toArray(String[]::new);

      String[] runtimeIndexes = getBinding().getDataset().getIndexing().getIndexes().stream().map(SovereignIndex::getDescription).map(desc -> {
        String cellName = desc.getCellName();
        String cellType = desc.getCellType().getJDKType().getSimpleName();
        String indexSetting = desc.getIndexSettings().toString();
        return cellName + "$$$" + cellType + "$$$" + indexSetting;
      }).toArray(String[]::new);

      String rootId = configuration.getDatasetConfiguration().getDiskResource().orElse("");
      String frsContainerName = PersistentStorageFactory.STORE_DIRECTORY;
      String frsLogName = PersistenceRoot.DATA;
      String restartableStoreId = retrieveRestartableStoreId(rootId, frsContainerName, frsLogName);

      return Collections.singleton(new Settings(getContext())
          .set("datasetName", getBinding().getAlias())
          .set("keyType", configuration.getKeyType().asEnum().name())
          .set("offheapResourceName", configuration.getDatasetConfiguration().getOffheapResource())
          .set("restartableStoreId", restartableStoreId)
          .set("restartableStoreRoot", rootId)
          .set("restartableStoreContainer", frsContainerName)
          .set("restartableStoreName", frsLogName)
          .set("concurrencyHint", configuration.getDatasetConfiguration().getConcurrencyHint().orElse(-1))
          .set("diskResource", configuration.getDatasetConfiguration().getDiskResource().orElse(null))
          .set("configuredIndexes", configuredIndexes)
          .set("runtimeIndexes", runtimeIndexes)
      );
    }
  }

  private static String retrieveRestartableStoreId(String rootId, String frsContainerName, String frsLogName) {
    return String.join("#",
        rootId,
        frsContainerName,
        frsLogName);
  }


}
