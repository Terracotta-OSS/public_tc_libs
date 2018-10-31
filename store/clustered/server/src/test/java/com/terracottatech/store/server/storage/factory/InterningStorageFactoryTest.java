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
import com.terracottatech.sovereign.impl.persistence.frs.SovereignFRSStorage;
import com.terracottatech.store.server.storage.configuration.PersistentStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.MemoryStorageConfiguration;
import com.terracottatech.store.server.storage.configuration.StorageConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class InterningStorageFactoryTest extends BufferResourceTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testInvalidConfig() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class);
    StorageFactory storageFactory = new InterningStorageFactory(underlying);

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap");
    when(underlying.getStorage(configuration)).thenThrow(new StorageFactoryException("Fail"));

    exception.expect(StorageFactoryException.class);
    storageFactory.getStorage(configuration);
  }

  @Test
  public void onlyCallsOnceWithValidResult() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    StorageFactory storageFactory = new InterningStorageFactory(underlying);

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap");
    assertNotNull(storageFactory.getStorage(configuration));
    assertNotNull(storageFactory.getStorage(configuration));

    verify(underlying, times(1)).getStorage(configuration);
  }

  @Test
  public void alwaysExceptionAfterClose() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    InterningStorageFactory storageFactory = new InterningStorageFactory(underlying);

    storageFactory.close();

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap");

    exception.expect(StorageFactoryException.class);
    storageFactory.getStorage(configuration);
  }

  @Test
  public void givesSameStorageForIdenticalConfig() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    InterningStorageFactory storageFactory = new InterningStorageFactory(underlying);

    StorageConfiguration configuration1 = new PersistentStorageConfiguration("abc", "offheap1");
    StorageConfiguration configuration2 = new PersistentStorageConfiguration("abc", "offheap1");

    SovereignStorage<?, ?> storage1 = storageFactory.getStorage(configuration1);
    SovereignStorage<?, ?> storage2 = storageFactory.getStorage(configuration2);

    assertNotNull(storage1);
    assertNotNull(storage2);
    assertThat(storage1, sameInstance(storage2));

    verify(underlying, times(1)).getStorage(configuration1);
    verifyNoMoreInteractions(underlying);
  }

  @Test
  public void differentOffheapResourcesAreValidForMemoryStorage() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    InterningStorageFactory storageFactory = new InterningStorageFactory(underlying);

    StorageConfiguration configuration1 = new MemoryStorageConfiguration("offheap1");
    StorageConfiguration configuration2 = new MemoryStorageConfiguration("offheap2");

    SovereignStorage<?, ?> storage1 = storageFactory.getStorage(configuration1);
    SovereignStorage<?, ?> storage2 = storageFactory.getStorage(configuration2);

    assertNotNull(storage1);
    assertNotNull(storage2);
    assertTrue(storage1 != storage2);

    verify(underlying, times(1)).getStorage(configuration1);
    verify(underlying, times(1)).getStorage(configuration2);
    verifyNoMoreInteractions(underlying);
  }

  @Test
  public void testRecoveryAfterFailureWithInvalidConfig() throws Exception {
    StorageConfiguration configuration1 = new PersistentStorageConfiguration("abc", "invalid-offheap");

    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    InterningStorageFactory storageFactory = new InterningStorageFactory(underlying);
    doThrow(StorageFactoryException.class).when(underlying).getStorage(configuration1);

    try {
      storageFactory.getStorage(configuration1);
    } catch (StorageFactoryException e) {
      // Expected
    }

    StorageConfiguration configuration2 = new PersistentStorageConfiguration("abc", "valid-offheap");
    storageFactory.getStorage(configuration2);  //This should not throw
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void cachedStorageClearedOnShutdown() throws Exception {
    InternalStorageFactory underlying = mock(InternalStorageFactory.class, RETURNS_DEEP_STUBS);
    SovereignStorage storage1 = mock(SovereignFRSStorage.class);
    SovereignStorage storage2 = mock(SovereignFRSStorage.class);
    when(underlying.getStorage(any(StorageConfiguration.class))).thenReturn(storage1, storage2);
    InterningStorageFactory storageFactory = new InterningStorageFactory(underlying);

    StorageConfiguration configuration = new PersistentStorageConfiguration("abc", "offheap1");
    SovereignStorage storage = storageFactory.getStorage(configuration);

    storageFactory.shutdownStorage(configuration, storage);

    assertThat(storageFactory.getStorage(configuration), not(sameInstance(storage)));
  }

}
