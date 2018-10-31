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
package com.terracottatech.ehcache.disk.config;

import org.ehcache.config.ResourceType;

/**
 * Types of enterprise disk resources.
 *
 * TODO: Currently only FRS, add Hybrid and other resource types later.
 *
 * @author RKAV
 */
public interface EnterpriseDiskResourceType<P extends EnterpriseDiskResourcePool> extends ResourceType<P> {
  final class Types {
    public static final EnterpriseDiskResourceType<FastRestartStoreResourcePool> FRS =
        new BaseEnterpriseResourceType<>("FRS", FastRestartStoreResourcePool.class);
    private static final class BaseEnterpriseResourceType<P extends EnterpriseDiskResourcePool>
        implements EnterpriseDiskResourceType<P> {
      private final String name;
      private final Class<P> resourcePoolClass;

      private BaseEnterpriseResourceType(final String name, final Class<P> resourcePoolClass) {
        this.name = name;
        this.resourcePoolClass = resourcePoolClass;
      }

      @Override
      public Class<P> getResourcePoolClass() {
        return resourcePoolClass;
      }

      @Override
      public boolean isPersistable() {
        return true;
      }

      @Override
      public boolean requiresSerialization() {
        return true;
      }

      @Override
      public int getTierHeight() {
        return 150;
      }

      @Override
      public String toString() {
        return "enterprise-" + this.name.toLowerCase();
      }
    }
  }
}
