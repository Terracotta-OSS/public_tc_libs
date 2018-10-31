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
package com.terracottatech.ehcache.common.frs.metadata;

import com.tc.classloader.CommonComponent;
import com.terracottatech.ehcache.common.function.Builder;
import com.terracottatech.ehcache.common.frs.RootPathProvider;
import com.terracottatech.ehcache.common.frs.metadata.internal.GlobalMetadataManager;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Meta data provider builder that builds a restartable FRS based metadata provider.
 *
 * @author RKAV
 */
@CommonComponent
public class MetadataProviderBuilder<K1, V1 extends RootConfiguration, V2 extends ChildConfiguration<K1>> implements
    Builder<MetadataProvider<K1, V1, V2>> {
  private final RootPathProvider rootPathProvider;
  private final Class<K1> parentKeyType;
  private final Class<V1> parentType;
  private final Class<V2> mainChildType;
  private final Set<Class<? extends ChildConfiguration<String>>> otherChildTypes;

  private MetadataProviderBuilder(final RootPathProvider rootPathProvider,
                                  final Class<K1> parentKeyType,
                                  final Class<V1> parentType,
                                  final Class<V2> mainChildType) {
    this.rootPathProvider = rootPathProvider;
    this.parentKeyType = parentKeyType;
    this.parentType = parentType;
    this.mainChildType = mainChildType;
    this.otherChildTypes = new HashSet<>();
  }

  private <V3 extends ChildConfiguration<String>> MetadataProviderBuilder(MetadataProviderBuilder<K1, V1, V2> original,
                                                                         Class<V3> otherChildType) {
    this.rootPathProvider = original.rootPathProvider;
    this.parentKeyType = original.parentKeyType;
    this.parentType = original.parentType;
    this.mainChildType = original.mainChildType;
    this.otherChildTypes = original.otherChildTypes;
    this.otherChildTypes.add(otherChildType);
  }

  public static <K1, V1 extends RootConfiguration, V2 extends ChildConfiguration<K1>>
  MetadataProviderBuilder<K1, V1, V2> metadata(RootPathProvider rootPathProvider,
                                               Class<K1> parentKeyType,
                                               Class<V1> parentType,
                                               Class<V2> childType) {
    return new MetadataProviderBuilder<>(rootPathProvider, parentKeyType, parentType, childType);
  }

  public <V3 extends ChildConfiguration<String>> MetadataProviderBuilder<K1, V1, V2> withOtherChildType(Class<V3> otherChildType) {
    return new MetadataProviderBuilder<>(this, otherChildType);
  }

  @Override
  public MetadataProvider<K1, V1, V2> build() {
    return new GlobalMetadataManager<>(
        rootPathProvider,
        parentKeyType,
        parentType,
        mainChildType,
        Collections.unmodifiableSet(otherChildTypes));
  }
}
