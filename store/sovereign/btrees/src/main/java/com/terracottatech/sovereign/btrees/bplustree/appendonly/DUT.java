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
package com.terracottatech.sovereign.btrees.bplustree.appendonly;

import com.terracottatech.sovereign.btrees.bplustree.model.BPlusTree;
import com.terracottatech.sovereign.btrees.stores.disk.DiskBufferProvider;
import com.terracottatech.sovereign.btrees.stores.disk.DiskFileSpace;
import com.terracottatech.sovereign.btrees.stores.disk.SimpleBlockBuffer;
import com.terracottatech.sovereign.btrees.stores.location.DirectoryStoreLocation;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.OffHeapStorageAreaBasedSpace;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;

import java.io.File;
import java.io.IOException;

/**
 * This is a BS class, a placeholder really, just hanging here with a bunch of example istantiations of trees.
 *
 * @author cschanck
 */
public final class DUT {

  private static String testDirectory = System.getProperty("test.directory", "e:\\auxindexes");

  private DUT() {
  }

  public static ABPlusTree<?> heap(int nodeSize) throws IOException {
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.heap(), ANode.maxSizeFor(nodeSize),
      128 * 1024 * 1024, 1024 * 1024 * 1024);
    ABPlusTree<?> bplustree = new ABPlusTree<>(space, null, null, nodeSize, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    return bplustree;
  }

  public static ABPlusTree<?> offheap(int nodeSize) throws IOException {
    int msize = ANode.maxSizeFor(nodeSize);
    PagedMemorySpace space = new PagedMemorySpace(PageSourceLocation.offheap(), msize, 128 * 1024 * 1024,
      1024 * 1024 * 1024);
    ABPlusTree<?> bplustree = new ABPlusTree<>(space, null, null, nodeSize, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    return bplustree;
  }

  public static ABPlusTree<?> offheapSA(int nodeSize) throws IOException {
    OffHeapStorageAreaBasedSpace space = new OffHeapStorageAreaBasedSpace(PageSourceLocation.offheap(),
      ANode.maxSizeFor(nodeSize));
    ABPlusTree<?> bplustree = new ABPlusTree<>(space, null, null, nodeSize, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    return bplustree;
  }

  public static ABPlusTree<?> diskUnlimitedUnmapped(int nodeSize) throws IOException {
    DirectoryStoreLocation dsl = new DirectoryStoreLocation(new File(testDirectory));
    dsl.destroy();
    PageSourceLocation bsl = PageSourceLocation.offheap();
    DiskFileSpace space = new DiskFileSpace(dsl, bsl,
      new SimpleBlockBuffer.Factory(bsl, new DiskBufferProvider.Unmapped(), 4 * 1024 * 1024), 256 * 1024 * 1024,
      1024 * 1024 * 1024, ANode.maxSizeFor(nodeSize));
    ABPlusTree<?> bplustree = new ABPlusTree<>(space, null, null, nodeSize, BPlusTree.DEFAULT_LONG_HANDLER, 1 * 1000);
    return bplustree;
  }

  public static ABPlusTree<?> diskUnlimitedMapped(int nodeSize) throws IOException {
    DirectoryStoreLocation dsl = new DirectoryStoreLocation(new File(testDirectory));
    dsl.destroy();
    PageSourceLocation bsl = PageSourceLocation.offheap();
    DiskFileSpace space = new DiskFileSpace(dsl, bsl,
      new SimpleBlockBuffer.Factory(bsl, new DiskBufferProvider.Mapped(), 4 * 1024 * 1024), 256 * 1024 * 1024,
      1024 * 1024 * 1024, ANode.maxSizeFor(nodeSize));
    ABPlusTree<?> bplustree = new ABPlusTree<>(space, null, null, nodeSize, BPlusTree.DEFAULT_LONG_HANDLER, 1 * 1000);
    return bplustree;
  }

}
