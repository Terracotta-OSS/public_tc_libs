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
package com.terracottatech.sovereign.common.splayed;

import org.terracotta.offheapstore.storage.StorageEngine;

/**
 * @author cschanck
 **/
class OwnerInterceptorStorageEngine<KK, VV> implements StorageEngine<KK, VV> {
  private final StorageEngine<KK, VV> delegate;

  OwnerInterceptorStorageEngine(StorageEngine<KK, VV> delegate) {
    this.delegate = delegate;
  }

  @Override
  public Long writeMapping(KK kk, VV vv, int i, int i1) {
    return delegate.writeMapping(kk, vv, i, i1);
  }

  @Override
  public void attachedMapping(long l, int i, int i1) {
    delegate.attachedMapping(l, i, i1);
  }

  @Override
  public void freeMapping(long l, int i, boolean b) {
    delegate.freeMapping(l, i, b);
  }

  @Override
  public VV readValue(long l) {
    return delegate.readValue(l);
  }

  @Override
  public boolean equalsValue(Object o, long l) {
    return delegate.equalsValue(o, l);
  }

  @Override
  public KK readKey(long l, int i) {
    return delegate.readKey(l, i);
  }

  @Override
  public boolean equalsKey(Object o, long l) {
    return delegate.equalsKey(o, l);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public long getAllocatedMemory() {
    return delegate.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return delegate.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return delegate.getVitalMemory();
  }

  @Override
  public long getDataSize() {
    return delegate.getDataSize();
  }

  @Override
  public void invalidateCache() {
    delegate.invalidateCache();
  }

  @Override
  public void bind(Owner owner) {
    // noop
  }

  @Override
  public void destroy() {
    delegate.destroy();
  }

  @Override
  public boolean shrink() {
    return delegate.shrink();
  }
}
