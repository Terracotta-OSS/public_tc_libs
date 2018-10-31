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
package com.terracottatech.testing.persistence;

import org.terracotta.entity.PlatformConfiguration;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceProvider;
import org.terracotta.entity.ServiceProviderCleanupException;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.management.service.monitoring.ManageableServerComponent;
import org.terracotta.persistence.IPlatformPersistence;

import com.tc.util.ProductCapabilities;
import com.terracottatech.br.ssi.BackupCapable;
import com.terracottatech.persistence.RestartablePlatformPersistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * DisabledPlatformPersistenceProvider
 */
public class DisabledPlatformPersistenceProvider implements ServiceProvider {
  @Override
  public boolean initialize(ServiceProviderConfiguration serviceProviderConfiguration, PlatformConfiguration platformConfiguration) {
    return false;
  }

  @Override
  public <T> T getService(long l, ServiceConfiguration<T> serviceConfiguration) {
    throw new UnsupportedOperationException("Not supposed to be in service!");
  }

  @Override
  public Collection<Class<?>> getProvidedServiceTypes() {
    List<Class<?>> supportedTypes = new ArrayList<>();
    supportedTypes.add(IPlatformPersistence.class);
    supportedTypes.add(RestartablePlatformPersistence.class);
    supportedTypes.add(ManageableServerComponent.class);
    supportedTypes.add(ProductCapabilities.class);
    supportedTypes.add(BackupCapable.class);
    return supportedTypes;
  }

  @Override
  public void prepareForSynchronization() throws ServiceProviderCleanupException {
    throw new UnsupportedOperationException("Not supposed to be in service");
  }
}
