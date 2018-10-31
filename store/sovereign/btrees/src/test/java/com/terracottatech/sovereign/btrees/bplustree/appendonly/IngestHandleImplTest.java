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
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.IngestHandle;
import com.terracottatech.sovereign.btrees.bplustree.model.Node;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.stores.SimpleStore;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.btrees.stores.memory.PagedMemorySpace;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IngestHandleImplTest {
  @Test
  public void testLotsOfCases() throws Exception {
    for (int i = 0; i < 2_000; i++) {
      runTest(10, i);
    }

    for (int i = 0; i < 5_000; i++) {
      runTest(15, i);
    }
  }

  private void runTest(int maxKeysPerNode, long countOfKeysToIngest) throws Exception {
    SimpleStore store = new PagedMemorySpace(PageSourceLocation.heap(), 4096, 256 * 1024, 1024 * 1024 * 1024);
    ABPlusTree<?> tree = new ABPlusTree<>(store, null, null, maxKeysPerNode, BPlusTree.DEFAULT_LONG_HANDLER, 0);
    SortedMap<Long, Long> expectedData = new TreeMap<>();

    IngestHandle ingest = tree.startBatch();
    for (long i = 0; i < countOfKeysToIngest; i++) {
      ingest.ingest(i * 2, i * 4);
      expectedData.put(i * 2, i * 4);
    }
    ingest.endBatch().commit();

    Collection<Node.VerifyError> errors = tree.readTx().verify();
    if (!errors.isEmpty()) {
      System.out.println("Failed running: " + maxKeysPerNode + ":" + countOfKeysToIngest);
      tree.dump(System.out);
      throw new RuntimeException(errors.iterator().next().toString());
    }

    checkData(tree, expectedData);
  }

  private void checkData(ABPlusTree<?> tree, SortedMap<Long, Long> expectedData) throws Exception {
    try (ReadTx<?> readTx = tree.readTx()) {
      Iterator<Map.Entry<Long, Long>> expectedIterator = expectedData.entrySet().iterator();
      Cursor cursor = readTx.cursor().first();

      while (expectedIterator.hasNext()) {
        assertTrue(cursor.hasNext());

        Map.Entry<Long, Long> expectedEntry = expectedIterator.next();
        BtreeEntry treeEntry = cursor.next();

        assertEquals((long) expectedEntry.getKey(), treeEntry.getKey());
        assertEquals((long) expectedEntry.getValue(), treeEntry.getValue());
      }

      assertFalse(cursor.hasNext());
    }
  }
}