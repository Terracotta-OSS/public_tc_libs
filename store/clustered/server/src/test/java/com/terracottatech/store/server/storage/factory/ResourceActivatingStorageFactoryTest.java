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

import com.terracottatech.sovereign.SovereignStorage;
import com.terracottatech.sovereign.resource.NamedBufferResources;
import com.terracottatech.store.StoreRuntimeException;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResourceActivatingStorageFactoryTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void activatesResource() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class);
    StorageConfiguration configuration = mock(StorageConfiguration.class);
    SovereignStorage storage = mock(SovereignStorage.class);
    NamedBufferResources bufferResource =  mock(NamedBufferResources.class);

    when(underlying.getStorage(configuration)).thenReturn(storage);
    when(storage.getBufferResource()).thenReturn(bufferResource);
    when(configuration.getOffheapResource()).thenReturn("offheap");

    ResourceActivatingStorageFactory storageFactory = new ResourceActivatingStorageFactory(underlying);
    storageFactory.getStorage(configuration);

    verify(bufferResource).activate("offheap");
  }

  @Test
  public void propagatesClose() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class);
    ResourceActivatingStorageFactory storageFactory = new ResourceActivatingStorageFactory(underlying);

    storageFactory.close();
    verify(underlying).close();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testGetStorageInvalidResource() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class);
    StorageConfiguration configuration = mock(StorageConfiguration.class);
    SovereignStorage storage = mock(SovereignStorage.class);
    NamedBufferResources bufferResource =  mock(NamedBufferResources.class);
    String resourceName = "invalid-offheap";

    when(underlying.getStorage(configuration)).thenReturn(storage);
    when(storage.getBufferResource()).thenReturn(bufferResource);
    doThrow(StoreRuntimeException.class).when(bufferResource).activate(resourceName);
    when(configuration.getOffheapResource()).thenReturn(resourceName);

    ResourceActivatingStorageFactory storageFactory = new ResourceActivatingStorageFactory(underlying);

    exception.expect(StorageFactoryException.class);
    exception.expectCause(instanceOf(StoreRuntimeException.class));
    storageFactory.getStorage(configuration);
  }

}
