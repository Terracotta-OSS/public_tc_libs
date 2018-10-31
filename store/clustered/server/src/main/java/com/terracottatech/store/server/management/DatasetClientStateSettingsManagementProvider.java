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

import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.ClientBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;

@Named("ClusteredDatasetClientStateSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class DatasetClientStateSettingsManagementProvider extends ClientBindingManagementProvider<ClientStateBinding> {

  DatasetClientStateSettingsManagementProvider() {
    super(ClientStateBinding.class);
  }

  @Override
  protected ExposedClientStateBinding internalWrap(Context context, ClientStateBinding managedObject) {
    return new ExposedClientStateBinding(context, managedObject);
  }

  private static class ExposedClientStateBinding extends ExposedClientBinding<ClientStateBinding> {

    ExposedClientStateBinding(Context context, ClientStateBinding clientBinding) {
      super(context.with("type", "DatasetClientState"), clientBinding);
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      return Collections.singleton(new Settings(getContext())
      );
    }
  }

}
