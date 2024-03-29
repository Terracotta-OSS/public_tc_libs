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
package com.terracottatech.ehcache.disk.service;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr.DefaultRestartableStoreContainerService;

/**
 * Factory creating FRS service.
 *
 * @author RKAV
 */
public class FastRestartStoreContainerServiceFactory implements ServiceFactory<FastRestartStoreContainerService> {
  @Override
  public FastRestartStoreContainerService create(ServiceCreationConfiguration<FastRestartStoreContainerService> configuration) {
    return new DefaultRestartableStoreContainerService();
  }

  @Override
  public Class<? extends FastRestartStoreContainerService> getServiceType() {
    return DefaultRestartableStoreContainerService.class;
  }
}