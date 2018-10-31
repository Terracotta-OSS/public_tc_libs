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
package com.terracottatech.sovereign.impl.persistence.base;

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.BtreeEntry;
import com.terracottatech.sovereign.btrees.bplustree.model.Cursor;
import com.terracottatech.sovereign.btrees.bplustree.model.KeyValueHandler;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import com.terracottatech.sovereign.btrees.bplustree.model.Tx;
import com.terracottatech.sovereign.btrees.bplustree.model.TxDecorator;
import com.terracottatech.sovereign.btrees.bplustree.model.WriteTx;
import com.terracottatech.sovereign.btrees.stores.SimpleStoreStats;
import com.terracottatech.sovereign.btrees.stores.location.PageSourceLocation;
import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.exceptions.SovereignExtinctionException;
import com.terracottatech.sovereign.impl.memory.SovereignRuntime;
import com.terracottatech.sovereign.impl.memory.storageengines.LongValueStorageEngines;
import com.terracottatech.sovereign.impl.memory.storageengines.PrimitivePortabilityImpl;
import com.terracottatech.sovereign.impl.utils.offheap.PowerOfTwoStore;
import com.terracottatech.sovereign.common.splayed.DynamicallySplayingOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author cschanck
 **/
public class OffheapAddressToLSNMap {

  private static final int KEYS_PER_NODE = 64;
  private final PageSource source;
  private final StorageEngine<Long, Long> engine;
  private final DynamicallySplayingOffHeapHashMap<Long, Long> addressToLSNMap;
  private final PageSourceLocation psl;
  private final SimpleStoreStats tstats;
  private ABPlusTree<TxDecorator> tree;
  private ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock rlock = rwlock.readLock();
  private ReentrantReadWriteLock.WriteLock wlock = rwlock.writeLock();
  private PowerOfTwoStore treeSpace;
  private boolean disposed = false;

  public OffheapAddressToLSNMap(SovereignRuntime<?> runtime, PageSource source, long totalSize) {
    this.source = source;
    this.engine = LongValueStorageEngines.factory(PrimitivePortabilityImpl.LONG, source, 1024 * 1024,
                                                  runtime.getConfig().maxResourceChunkSize());
    this.addressToLSNMap = new DynamicallySplayingOffHeapHashMap<>(hashcode -> MiscUtils.stirHash(hashcode), source,
                                                                   engine);
    this.psl = new PageSourceLocation(source, totalSize, runtime.getConfig().maxResourceChunkSize());
    createTree();
    this.tstats = tree.getTreeStore().getStats();
  }

  public long getOccupiedMemoryStorage() {
    return addressToLSNMap.getOccupiedDataSize() + addressToLSNMap.getOccupiedOverheadMemory() + tstats.getUserBytes();
  }

  public long getReservedMemoryStorage() {
    return addressToLSNMap.getReservedDataSize() + addressToLSNMap.getReservedOverheadMemory() + tstats.getAllocatedBytes();
  }

  private void createTree() {
    try {
      this.treeSpace = new PowerOfTwoStore(psl, 4096, 1024 * 1024, 1024 * 1024);

      this.tree = new ABPlusTree<>(treeSpace, null, null, ABPlusTree.calculateMaxKeyCountForPageSize(4096),
                                   new KeyValueHandler<TxDecorator>() {

                                     @Override
                                     public long compareLongKeys(Tx<TxDecorator> tx, long target, long k2) throws IOException {
                                       return Long.compare(target, k2);
                                     }

                                     @Override
                                     public long compareObjectKeys(Tx<TxDecorator> tx, Object target, long k2) throws IOException {
                                       return Long.compare((Long) target, k2);
                                     }
                                   }, 0);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public AbstractMap.SimpleEntry<Long, Long> firstLSNAndAddress() {
    rlock.lock();
    try {
      if (this.disposed) {
        return null;
      }

      try (ReadTx<TxDecorator> rtx = tree.readTx()) {
        Cursor c = rtx.cursor().first();
        if (c.hasNext()) {
          BtreeEntry ent = c.next();
          return new AbstractMap.SimpleEntry<>(ent.getKey(), ent.getValue());
        }
        return null;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } finally {
      rlock.unlock();
    }
  }

  public void put(long address, long lsn) {
    wlock.lock();
    try {
      if (this.disposed) {
        return;
      }
      WriteTx<TxDecorator> tx = tree.writeTx();
      try {
        Long old = addressToLSNMap.put(address, lsn);
        if (old != null) {
          // delete the old lsn
          tx.delete(old.longValue());
        }
        tx.insert(lsn, address);
        tx.commit();
      } catch (IOException e) {
        // best effort
        try {
          tx.discard();
        } catch (IOException e1) {
        }
      } finally {
        try {
          tx.close();
        } catch (Throwable e) {
          throw SovereignExtinctionException.ExtinctionType.PERSISTENCE_ADD_FAILURE.exception(e);
        }
      }
    } finally {
      wlock.unlock();
    }
  }

  public void remove(long address) {
    wlock.lock();

    try {
      if (this.disposed) {
        return;
      }
      WriteTx<TxDecorator> tx = tree.writeTx();
      try {
        Long lsn = addressToLSNMap.remove(address);
        if (lsn != null) {
          tx.delete(lsn.longValue());
        }
        tx.commit();
      } catch (Exception e) {
        try {
          // best effort
          tx.discard();
        } catch (Exception e1) {
        }
      } finally {
        try {
          tx.close();
        } catch (Throwable e) {
          throw SovereignExtinctionException.ExtinctionType.PERSISTENCE_DELETE_FAILURE.exception(e);
        }
      }

    } finally {
      wlock.unlock();
    }
  }

  public Long getLsn(long address) {
    rlock.lock();
    try {
      if (this.disposed) {
        return null;
      }
      return addressToLSNMap.get(address);
    } finally {
      rlock.unlock();
    }
  }

  public Long getAddressForLSN(long lsn) {
    rlock.lock();
    try {
      if (this.disposed) {
        return null;
      }
      try (ReadTx<TxDecorator> rtx = tree.readTx()) {
        Cursor c = rtx.cursor();
        if (c.scanTo(lsn)) {
          long ret = c.next().getValue();
          return ret;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } finally {
      rlock.unlock();
    }
    return null;
  }

  public long size() {
    rlock.lock();
    try {
      if (this.disposed) {
        return 0;
      }
      try (ReadTx<TxDecorator> rtx = tree.readTx()) {
        return rtx.size();
      }
    } finally {
      rlock.unlock();
    }
  }

  public void dispose() {
    wlock.lock();
    try {
      if (this.disposed) {
        return;
      }
      this.disposed = true;
      addressToLSNMap.destroy();
      closeTree();
    } finally {
      wlock.unlock();
    }
  }

  public void clear() {
    wlock.lock();
    try {
      if (this.disposed) {
        return;
      }
      addressToLSNMap.clear();
      closeTree();
      createTree();
    } finally {
      wlock.unlock();
    }
  }

  private void closeTree() {
    try {
      tree.close();
    } catch (IOException e) {
      // TODO log;
    }
    treeSpace.close();
  }

  public List<String> verifyInternalConsistency() throws IOException {
    ArrayList<String> ret = new ArrayList<String>();
    wlock.lock();
    try {
      if (this.disposed) {
        return null;
      }
      try (ReadTx<TxDecorator> rtx = tree.readTx()) {

        Cursor c = rtx.cursor().first();
        while (c.hasNext()) {
          BtreeEntry p = c.next();
          long adr = p.getValue();
          if (addressToLSNMap.get((Long) adr) == null) {
            ret.add("Address: " + adr + " missing from map");
          }
        }
      }
    } finally {
      wlock.unlock();
    }
    return ret;
  }
}
