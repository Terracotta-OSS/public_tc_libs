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
package com.terracottatech.ehcache.clustered.client.internal;

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryFactory;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * Activates management service automatically for EE.
 *
 * @author Mathieu Carbou
 */
public class EnterpriseManagementRegistryFactory extends DefaultManagementRegistryFactory {

  @Override
  public boolean isMandatory() {
    return true;
  }

  @Override
  public int rank() {
    return 10;
  }

  @Override
  public ManagementRegistryService create(ServiceCreationConfiguration<ManagementRegistryService> configuration) {
    if (configuration == null) {
      return new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration()
          .addTag("ehcache-ee"));
    }
    if (configuration instanceof ManagementRegistryServiceConfiguration) {
      return new DefaultManagementRegistryService((ManagementRegistryServiceConfiguration) configuration);
    }
    throw new IllegalArgumentException("Expected a configuration of type " + ManagementRegistryServiceConfiguration.class.getSimpleName() + " but got " + configuration.getClass().getSimpleName());
  }

}
