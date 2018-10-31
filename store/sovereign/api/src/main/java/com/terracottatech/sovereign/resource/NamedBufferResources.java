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
package com.terracottatech.sovereign.resource;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.store.StoreRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class NamedBufferResources implements SovereignBufferResource {
  private final Map<String, SovereignBufferResource> bufferResources;
  private AtomicReference<String> activationName = new AtomicReference<>();
  private volatile SovereignBufferResource activeBufferResource;

  public NamedBufferResources(Map<String, ? extends SovereignBufferResource> bufferResources) {
    this.bufferResources = new HashMap<>(bufferResources);
  }

  public NamedBufferResources(Map<String, ? extends SovereignBufferResource> bufferResources, String... activations) {
    this(bufferResources);

    for (String activation : activations) {
      activate(activation);
    }
  }

  public void activate(String offheapResourceName) {
    SovereignBufferResource bufferResource = bufferResources.get(offheapResourceName);

    if (bufferResource == null) {
      throw new StoreRuntimeException("Unknown offheap resource: " + offheapResourceName);
    }

    boolean activated = activationName.compareAndSet(null, offheapResourceName);

    if (!activated) {
      String activeOffheapResourceName = activationName.get();
      assert(activeOffheapResourceName != null);

      if (!activeOffheapResourceName.equals(offheapResourceName)) {
        throw new StoreRuntimeException("You cannot use more than one resource. Attempt to use: " + offheapResourceName + " when already using: " + activeOffheapResourceName);
      }
    }

    activeBufferResource = bufferResource;
  }

  @Override
  public long getSize() {
    checkActivated();
    return activeBufferResource.getSize();
  }

  @Override
  public long getAvailable() {
    checkActivated();
    return activeBufferResource.getAvailable();
  }

  @Override
  public boolean reserve(int bytes) {
    checkActivated();
    return activeBufferResource.reserve(bytes);
  }

  @Override
  public void free(long bytes) {
    checkActivated();
    activeBufferResource.free(bytes);
  }

  @Override
  public MemoryType type() {
    return MemoryType.OFFHEAP;
  }

  @Override
  public String getName() {
    checkActivated();
    return activationName.get();
  }

  private void checkActivated() {
    if (activeBufferResource == null) {
      throw new StoreRuntimeException("No resource activated yet");
    }
  }

  public String toString() {
    SovereignBufferResource currentBufferResource = activeBufferResource;
    if (currentBufferResource == null) {
      return getClass().getName().substring(1 + getClass().getName().lastIndexOf('.'))
          + "[<inactive>]{type=<inactive>, capacity=<unknown>, available=<unknown>}";
    } else {
      return getClass().getName().substring(1 + getClass().getName().lastIndexOf('.'))
          + "[" + activationName.get() + "]"
          + "{type=" + currentBufferResource.type()
          + ", capacity=" + currentBufferResource.getSize()
          + ", available=" + currentBufferResource.getAvailable() + '}';
    }
  }
}
