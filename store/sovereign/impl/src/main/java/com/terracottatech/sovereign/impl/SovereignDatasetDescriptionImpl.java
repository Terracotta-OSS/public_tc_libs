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
package com.terracottatech.sovereign.impl;

import com.terracottatech.sovereign.description.SovereignDatasetDescription;
import com.terracottatech.sovereign.impl.indexing.SimpleIndexDescription;
import com.terracottatech.sovereign.indexing.SovereignIndex;
import com.terracottatech.sovereign.time.TimeReference;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * @author cschanck
 */
public class SovereignDatasetDescriptionImpl<T extends Comparable<T>, Z extends TimeReference<Z>>
    implements SovereignDatasetDescription {

  private static final long serialVersionUID = 1L;
  private final SovereignDataSetConfig<T, Z> config;
  private final UUID uuid;
  private final SimpleIndexDescription<?>[] indexes;
  private final String alias;

  @SuppressWarnings("unchecked")
  public SovereignDatasetDescriptionImpl(SovereignDatasetImpl<T> dataset) {
    this(dataset.getUUID(),
            makeAlias(dataset.getConfig().getAlias(), dataset.getUUID()),
            (SovereignDataSetConfig<T, Z>) dataset.getConfig().duplicate().freeze(),
            dataset.getIndexing().getIndexes().stream().map(SovereignIndex::getDescription).toArray(SimpleIndexDescription[]::new));
  }

  public static <T extends Comparable<T>, Z extends TimeReference<Z>> SovereignDatasetDescriptionImpl<T, Z> normalizeUUID(SovereignDatasetDescriptionImpl<T, Z> description, UUID requiredDatasetUUID) {
    return new SovereignDatasetDescriptionImpl<>(requiredDatasetUUID, description.alias, description.config, description.indexes);
  }

  private SovereignDatasetDescriptionImpl(UUID uuid, String alias, SovereignDataSetConfig<T, Z> config, SimpleIndexDescription<?>[] indexes) {
    this.uuid = uuid;
    this.alias = alias;
    this.config = config;
    this.indexes = indexes;
  }

  private static String makeAlias(String possibleAlias, UUID uuid) {
    return possibleAlias == null ? uuid.toString() : possibleAlias;
  }

  public SovereignDataSetConfig<T, Z> getConfig() {
    return config;
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public Optional<String> getOffheapResourceName() {
    return Optional.ofNullable(config.getOffheapResourceName());
  }

  @Override
  public UUID getUUID() {
    return uuid;
  }

  @Override
  public List<SimpleIndexDescription<?>> getIndexDescriptions() {
    return Collections.unmodifiableList(Arrays.asList(indexes));
  }

  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);

    pw.println("Dataset: " + getUUID() + "alias: " + getAlias() + " type: " + config.getType());
    for (SimpleIndexDescription<?> si : getIndexDescriptions()) {
      pw.println(si);
    }
    pw.flush();
    return sw.toString();
  }
}
