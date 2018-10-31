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
package com.terracottatech.store.server.storage.factory;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.store.server.storage.offheap.BufferResourceFactory;
import com.terracottatech.store.server.storage.offheap.UnknownResourceException;

import org.junit.Before;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferResourceTest {
  protected BufferResourceFactory bufferResourceFactory;

  @Before
  public void before() throws Exception {
    bufferResourceFactory = mock(BufferResourceFactory.class);
    SovereignBufferResource bufferResource = mock(SovereignBufferResource.class);
    when(bufferResource.type()).thenReturn(SovereignBufferResource.MemoryType.OFFHEAP);
    when(bufferResource.reserve(anyInt())).thenReturn(true);
    when(bufferResourceFactory.get("offheap")).thenReturn(bufferResource);

    SovereignBufferResource anotherBufferResource = mock(SovereignBufferResource.class);
    when(anotherBufferResource.type()).thenReturn(SovereignBufferResource.MemoryType.OFFHEAP);
    when(anotherBufferResource.reserve(anyInt())).thenReturn(true);
    when(bufferResourceFactory.get("another-offheap")).thenReturn(anotherBufferResource);

    when(bufferResourceFactory.get("unknown")).thenThrow(UnknownResourceException.class);
  }
}
