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
package com.terracottatech.ehcache.clustered.server.services.frs.management;

import com.tc.classloader.CommonComponent;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.AliasBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;

@Named("EhcachePersistenceSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
@CommonComponent
public class EhcachePersistenceSettingsManagementProvider extends AliasBindingManagementProvider<RestartStoreBinding> {

  public EhcachePersistenceSettingsManagementProvider() {
    super(RestartStoreBinding.class);
  }

  @Override
  protected ExposedRestartStoreBinding internalWrap(Context context, RestartStoreBinding managedObject) {
    return new ExposedRestartStoreBinding(context, managedObject);
  }

  private static class ExposedRestartStoreBinding extends ExposedAliasBinding<RestartStoreBinding> {

    ExposedRestartStoreBinding(Context context, RestartStoreBinding binding) {
      super(context.with("type", "EhcacheRestartStore"), binding);
    }

    @Override
    public Collection<? extends Settings> getDescriptors() {
      return Collections.singleton(new Settings(getContext())
          .set("restartableStoreId", getBinding().getRestartableStoreId())
          .set("restartableStoreRoot", getBinding().getRestartableStoreRoot()) // matches dataRootId
          .set("restartableStoreContainer", getBinding().getRestartableStoreContainer())
          .set("restartableStoreName", getBinding().getRestartableLogName())
          .set("restartableStoreLocation", getBinding().getRestartableStorageLocation())
      );
    }
  }

}
