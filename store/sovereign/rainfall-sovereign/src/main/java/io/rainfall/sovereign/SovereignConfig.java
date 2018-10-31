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
package io.rainfall.sovereign;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;

import io.rainfall.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cschanck
 **/
public class SovereignConfig<K extends Comparable<K>> extends Configuration implements Iterable<SovereignDatasetImpl<K>> {
  private ConcurrentHashMap<UUID, SovereignDatasetImpl<K>> datasetMap = new ConcurrentHashMap<>();
  private SovereignDataset.Durability addDurability;
  private SovereignDataset.Durability mutateDurability;
  private SovereignDataset.Durability deleteDurability;
  private ABPlusTree<?> tree = null;
  private List<String> descr = null;

  public void addDataset(SovereignDatasetImpl<K> ds) {
    if (datasetMap.putIfAbsent(ds.getUUID(), ds) != null) {
      throw new IllegalStateException("attempt to add existent dataset");
    }
  }

  public SovereignDatasetImpl<?> getDataset(UUID uuid) {
    return datasetMap.get(uuid);
  }

  public void removeDataset(UUID uuid) {
    datasetMap.remove(uuid);
  }

  @Override
  public Iterator<SovereignDatasetImpl<K>> iterator() {
    return datasetMap.values().iterator();
  }

  public void setAddDurability(SovereignDataset.Durability addDurability) {
    this.addDurability = addDurability;
  }

  public void setMutateDurability(SovereignDataset.Durability mutateDurability) {
    this.mutateDurability = mutateDurability;
  }

  public void setDeleteDurability(SovereignDataset.Durability deleteDurability) {
    this.deleteDurability = deleteDurability;
  }

  public SovereignDataset.Durability getAddDurability() {
    return addDurability;
  }

  public SovereignDataset.Durability getMutateDurability() {
    return mutateDurability;
  }

  public SovereignDataset.Durability getDeleteDurability() {
    return deleteDurability;
  }

  public SovereignConfig<K> addDurability(final SovereignDataset.Durability addDurability) {
    this.addDurability = addDurability;
    return this;
  }

  public SovereignConfig<K> mutateDurability(final SovereignDataset.Durability mutateDurability) {
    this.mutateDurability = mutateDurability;
    return this;
  }

  public SovereignConfig<K> deleteDurability(final SovereignDataset.Durability deleteDurability) {
    this.deleteDurability = deleteDurability;
    return this;
  }

  public void setAllDurability(SovereignDataset.Durability d) {
    addDurability(d).deleteDurability(d).mutateDurability(d);
  }

  public SovereignConfig<K> allDurability(final SovereignDataset.Durability d) {
    setAddDurability(d);
    return this;
  }

  public void setTree(ABPlusTree<?> tree) {
    this.tree = tree;
  }

  public SovereignConfig<K> tree(final ABPlusTree<?> tree) {
    this.tree = tree;
    return this;
  }

  public ABPlusTree<?> getTree() {
    return tree;
  }

  @Override
  public List<String> getDescription() {
    if (descr == null) {
      return Collections.singletonList("Sovereign test");
    }
    return descr;
  }

  public void setDescription(String[] descrs) {
    this.descr = Arrays.asList(descrs);
  }

  public void setDescription(String d) {
    this.descr = Collections.singletonList(d);
  }
}
