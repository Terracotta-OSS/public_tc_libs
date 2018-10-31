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

package com.terracottatech.ehcache.clustered.server.management;

import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.AliasBindingManagementProvider;

import com.terracottatech.ehcache.clustered.common.RestartConfiguration;
import com.terracottatech.ehcache.clustered.server.state.EnterpriseEhcacheStateService;

import java.util.Collection;
import java.util.Collections;

@Named("ClusterTierManagerSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
public class EnterpriseClusterTierManagerSettingsManagementProvider extends AliasBindingManagementProvider<EnterpriseClusterTierManagerBinding> {

  public EnterpriseClusterTierManagerSettingsManagementProvider() {
    super(EnterpriseClusterTierManagerBinding.class);
  }

  @Override
  protected ExposedAliasBinding<EnterpriseClusterTierManagerBinding> internalWrap(final Context context, final EnterpriseClusterTierManagerBinding managedObject) {
    return new ExposedClusterTierManagerBinding(context, managedObject);
  }

  private static class ExposedClusterTierManagerBinding extends ExposedAliasBinding<EnterpriseClusterTierManagerBinding> {

    public ExposedClusterTierManagerBinding(final Context context, final EnterpriseClusterTierManagerBinding binding) {
      super(context, binding);
    }

    @Override
    public Context getContext() {
      return super.getContext().with("type", "ClusterTierManager");
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      EnterpriseEhcacheStateService stateService = getBinding().getValue();
      RestartConfiguration restartConfiguration = stateService.getRestartConfiguration();
      Settings settings = new Settings(getContext())
          .set("clusterTierManager", getBinding().getAlias())
          .set("defaultServerResource", stateService.getDefaultServerResource());
      if (restartConfiguration != null) {
        // generate a key to find the right restartable store used by this cluster tier manager
        String restartableStoreId = getRestartableStoreId(restartConfiguration);
        settings
            .set("restartableStoreId", restartableStoreId) // matches 'alias' in exposed objects "EhcacheRestartStore" in provider "EhcachePersistenceSettings"
            .set("restartableStoreRoot", restartConfiguration.getRestartableLogRoot()) // matches dataRootId config
            .set("restartableStoreContainer", restartConfiguration.getRestartableLogContainer())
            .set("restartableStoreName", restartConfiguration.getRestartableLogName())
            .set("offheapMode", restartConfiguration.getOffHeapMode());
      }
      return Collections.singleton(settings);
    }
  }

  private static String getRestartableStoreId(RestartConfiguration restartConfiguration) {
    return String.join("#",
        restartConfiguration.getRestartableLogRoot(),
        restartConfiguration.getRestartableLogContainer(),
        restartConfiguration.getRestartableLogName());
  }
}
