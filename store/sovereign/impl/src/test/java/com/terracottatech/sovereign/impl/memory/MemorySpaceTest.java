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

package com.terracottatech.sovereign.impl.memory;

/**
 * @author Clifford W. Johnson
 */
public class MemorySpaceTest {
  // TODO XXX
//
//  private MemorySpace space = new MemorySpace(new OffHeapBufferSource());
//
//  @Test
//  public void testDrop() throws Exception {
//    final Field pageSourceField = MemorySpace.class.getDeclaredField("pageSource");
//    pageSourceField.setAccessible(true);
//    final Field mapsField = MemorySpace.class.getDeclaredField("maps");
//    mapsField.setAccessible(true);
//    final Field containersField = MemorySpace.class.getDeclaredField("containers");
//    containersField.setAccessible(true);
//
//    final DataContainer<Record<String>> recordContainer =
//        this.space.createContainer();
//    assertFalse(recordContainer.isDisposed());
//
//    final CompositeDataContainer<String, PersistentRecord<String>> compositeContainer =
//        this.space.createCompositeContainer(
//            (PersistentRecord<String> model,
//             DataContainer<PersistentRecord<String>> store,
//             Locator items) -> new VersionedRecordData<>(store, items, 1, t -> true));
//    assertFalse(compositeContainer.isDisposed());
//
//    final IndexMap<String> unorderedIndex = this.space.createMap(new IndexMapConfig<String>() {
//      @Override
//      public boolean isSortedMap() {
//        return false;
//      }
//
//      @Override
//      public Type<String> getType() {
//        return Type.STRING;
//      }
//
//      @Override
//      public DataContainer getContainer() {
//        return recordContainer;
//      }
//    });
//
//    final IndexMap<String> orderedIndex = this.space.createMap(new IndexMapConfig<String>() {
//      @Override
//      public boolean isSortedMap() {
//        return true;
//      }
//
//      @Override
//      public Type<String> getType() {
//        return Type.STRING;
//      }
//
//      @Override
//      public DataContainer getContainer() {
//        return recordContainer;
//      }
//    });
//
//    assertFalse(this.space.isDropped());
//    this.space.drop();
//    assertTrue(this.space.isDropped());
//
//    assertNull(pageSourceField.get(this.space));
//
//    assertTrue(recordContainer.isDisposed());
//    assertTrue(compositeContainer.isDisposed());
//
//    /*
//     * The IndexMaps lack an isDropped method so we'll check they've been dropped by
//     * attempting a get.
//     */
//    final Context context = mock(Context.class);
//    for (final IndexMap<String> map : Arrays.asList(unorderedIndex, orderedIndex)) {
//      try {
//        map.get(context, "echidna");
//        fail(String.format("%s is not dropped", map.getClass().getSimpleName()));
//      } catch (IllegalStateException e) {
//        // Expected
//      }
//
//      verifyZeroInteractions(context);
//    }
//  }
//
//  @Test
//  public void testDroppedDrop() throws Exception {
//    this.space.drop();
//    this.space.drop();    // Second drop should not fail
//  }
//
//  @Test
//  public void testDroppedCreateCompositeContainer() throws Exception {
//    this.space.drop();
//
//    @SuppressWarnings("unchecked")
//    final CompositeFactory<String, PersistentRecord<String>> wrapper =
//        mock(CompositeFactory.class);   // unchecked
//    try {
//      this.space.createCompositeContainer(wrapper);
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//
//    verifyZeroInteractions(wrapper);
//  }
//
//  @Test
//  public void testDroppedCreateContainer() throws Exception {
//    this.space.drop();
//
//    try {
//      this.space.createContainer();
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//  }
//
//  @Test
//  public void testDroppedCreateMap() throws Exception {
//    this.space.drop();
//
//    @SuppressWarnings("unchecked")
//    final IndexMapConfig<String> indexMapConfig = mock(IndexMapConfig.class);   // unchecked
//    when(indexMapConfig.isSortedMap()).thenReturn(false);
//    try {
//      this.space.createMap(indexMapConfig);
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//
//    verifyZeroInteractions(indexMapConfig);
//  }
//
//  @Test
//  public void testDroppedCreateSortedMap() throws Exception {
//    this.space.drop();
//
//    @SuppressWarnings("unchecked")
//    final IndexMapConfig<String> indexMapConfig = mock(IndexMapConfig.class);   // unchecked
//    when(indexMapConfig.isSortedMap()).thenReturn(true);
//    try {
//      this.space.createMap(indexMapConfig);
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//
//    verifyZeroInteractions(indexMapConfig);
//  }
//
//  @Test
//  public void testDroppedGetCapacity() throws Exception {
//    this.space.drop();
//    this.space.getCapacity();
//  }
//
//  @Test
//  public void testDroppedGetReserved() throws Exception {
//    this.space.drop();
//
//    try {
//      this.space.getReserved();
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//  }
//
//  @Test
//  public void testDroppedGetUsed() throws Exception {
//    this.space.drop();
//
//    try {
//      this.space.getUsed();
//      fail();
//    } catch (IllegalStateException e) {
//      // Expected
//    }
//  }
//
//  @Test
//  public void testDroppedIsReadOnly() throws Exception {
//    this.space.drop();
//    this.space.isReadOnly();
//  }
}
