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
package com.terracottatech.sovereign.impl.persistence.frs;

import com.terracottatech.sovereign.impl.memory.AbstractRecordContainer;
import com.terracottatech.sovereign.impl.memory.ShardSpec;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.model.PersistableDataContainer;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;

/**
 * @author cschanck
 **/
public class FRSRecordContainer<K extends Comparable<K>>
  extends AbstractRecordContainer<K> implements PersistableDataContainer<K, FRSBroker> {
  private final FRSBufferContainer bContainer;

  public FRSRecordContainer(ShardSpec shardSpec, SovereignRuntime<K> runtime, PageSource source) {
    super(shardSpec, runtime);
    bContainer = new FRSBufferContainer(shardSpec, runtime(), source);
  }

  @Override
  public long getAllocatedPersistentSupportStorage() {
    return getBufferContainer().getAllocatedPersistentSupportStorage();
  }

  @Override
  public long getOccupiedPersistentSupportStorage() {
    return getBufferContainer().getOccupiedPersistentSupportStorage();
  }

  @Override
  public long getPersistentBytesUsed() {
    return getBufferContainer().getPersistentBytesUsed();
  }

  @Override
  public FRSBufferContainer getBufferContainer() {
    return bContainer;
  }

  @Override
  public void setBroker(FRSBroker tap) {
    getBufferContainer().setBroker(tap);
  }

  @Override
  public void finishRestart() {
    getBufferContainer().finishRestart();
  }

}
