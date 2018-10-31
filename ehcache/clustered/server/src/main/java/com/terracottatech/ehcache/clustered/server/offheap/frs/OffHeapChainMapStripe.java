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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.frs.object.AbstractObjectManagerStripe;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.RestartableObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractHashCodeFromKeyBuffer;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractKeyFromKeyBuffer;

/**
 * A simple single segment stripe per chain map.
 *
 * @param <I>  type of identifier
 */
public class OffHeapChainMapStripe<I, K> extends AbstractObjectManagerStripe<I, ByteBuffer, ByteBuffer>
    implements RestartableObject<I, ByteBuffer, ByteBuffer> {
  private final I identifier;
  private final KeyToSegment<K> mapper;
  private final List<OffHeapChainMap<K>> segments;
  private final List<RestartableChainStorageEngine<I, K>> restartableStorageEngines;
  private final Portability<K> keyPortability;

  public OffHeapChainMapStripe(I identifier, KeyToSegment<K> mapper, PageSource source,
                               Portability<K> keyPortability,
                               Factory<? extends RestartableChainStorageEngine<I, K>> storageEngineFactory) {
    this.identifier = identifier;
    this.mapper = mapper;
    this.segments = Collections.synchronizedList(new ArrayList<>(mapper.getSegments()));
    this.restartableStorageEngines = Collections.synchronizedList(new ArrayList<>(mapper.getSegments()));
    this.keyPortability = keyPortability;
    for (int i = 0; i < mapper.getSegments(); i++) {
      OffHeapChainMap<K> map = new OffHeapChainMap<>(source, storageEngineFactory);
      segments.add(map);
      @SuppressWarnings("unchecked")
      RestartableChainStorageEngine<I, K> storageEngine = (RestartableChainStorageEngine<I, K>) map.getStorageEngine();
      restartableStorageEngines.add(storageEngine);
    }
  }

  @Override
  public Collection<ObjectManagerSegment<I, ByteBuffer, ByteBuffer>> getSegments() {
    return Collections.unmodifiableList(restartableStorageEngines);
  }

  public List<OffHeapChainMap<K>> segments() {
    return segments;
  }

  @Override
  protected ObjectManagerSegment<I, ByteBuffer, ByteBuffer> getSegmentFor(int hash, ByteBuffer frsBinaryKey) {
    final K key = keyPortability.decode(extractKeyFromKeyBuffer(frsBinaryKey));
    return restartableStorageEngines.get(mapper.getSegmentForKey(key));
  }

  @Override
  protected int extractHashCode(ByteBuffer frsBinaryKey) {
    return extractHashCodeFromKeyBuffer(frsBinaryKey);
  }

  @Override
  public void delete() {
    //no-op
  }

  @Override
  public I getId() {
    return identifier;
  }

  @Override
  public ObjectManagerStripe<I, ByteBuffer, ByteBuffer> getObjectManagerStripe() {
    return this;
  }
}
