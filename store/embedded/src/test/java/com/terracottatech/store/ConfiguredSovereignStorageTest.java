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
package com.terracottatech.store;

import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.resource.NamedBufferResources;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ConfiguredSovereignStorageTest {
  @Test
  public void setAndGet() throws Exception {
    SovereignStorage<?, ?> storage = mock(SovereignStorage.class);
    NamedBufferResources bufferResource = mock(NamedBufferResources.class);
    ConfiguredSovereignStorage configuredSovereignStorage = new ConfiguredSovereignStorage(bufferResource, storage);

    configuredSovereignStorage.activate("abc");

    assertEquals(storage, configuredSovereignStorage.getStorage());

    verify(bufferResource).activate("abc");
    verifyNoMoreInteractions(storage);
  }

  @Test
  public void closeCorrectly() throws Exception {
    SovereignStorage<?, ?> storage = mock(SovereignStorage.class);
    NamedBufferResources bufferResource = mock(NamedBufferResources.class);
    ConfiguredSovereignStorage configuredSovereignStorage = new ConfiguredSovereignStorage(bufferResource, storage);

    configuredSovereignStorage.close();

    verify(storage).shutdown();
    verifyNoMoreInteractions(storage);
  }
}
