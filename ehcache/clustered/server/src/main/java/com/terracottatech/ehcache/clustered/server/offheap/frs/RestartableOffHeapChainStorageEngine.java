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
package com.terracottatech.ehcache.clustered.server.offheap.frs;

import org.ehcache.clustered.server.offheap.InternalChain;
import org.ehcache.clustered.server.offheap.OffHeapChainStorageEngine;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import static com.terracottatech.ehcache.clustered.server.offheap.frs.LinkedChainNode.EMPTY_LINKED_CHAIN_NODE;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.LinkedChainNode.NULL_ENCODING;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.encodeKey;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractKeyFromKeyBuffer;

public class RestartableOffHeapChainStorageEngine<I, K> extends OffHeapChainStorageEngine<K>
    implements RestartableChainStorageEngine<I, K> {
  private final I identifier;
  private final RestartStore<I, ByteBuffer, ByteBuffer> dataStorage;
  private final boolean synchronous;

  private long first = NULL_ENCODING;
  private long last = NULL_ENCODING;
  private ObjectManagerEntry<I, ByteBuffer, ByteBuffer> compactingEntry;
  private volatile boolean bypassEngineCommands = false;

  public static <I, K> Factory<? extends RestartableChainStorageEngine<I, K>>
  createFactory(final PageSource source,
                final Portability<? super K> keyPortability,
                final int minPageSize, final int maxPageSize,
                final boolean thief, final boolean victim,
                final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> dataStorage,
                final boolean synchronous) {
    return (Factory<RestartableOffHeapChainStorageEngine<I, K>>)() ->
        new RestartableOffHeapChainStorageEngine<>(source, keyPortability,
        minPageSize, maxPageSize, thief, victim, identifier, dataStorage, synchronous);
  }

  private RestartableOffHeapChainStorageEngine(PageSource source, Portability<? super K> keyPortability, int minPageSize,
                                              int maxPageSize, boolean thief, boolean victim,
                                              I identifier,
                                              RestartStore<I, ByteBuffer, ByteBuffer> dataStorage,
                                              boolean synchronous) {
    super(source, keyPortability, minPageSize, maxPageSize, thief, victim, EMPTY_LINKED_CHAIN_NODE);
    this.identifier = identifier;
    this.dataStorage = dataStorage;
    this.synchronous = synchronous;
  }

  @Override
  public Long writeMapping(K key, InternalChain value, int hash, int metadata) {
    bypassEngineCommands = true;
    try {
      return super.writeMapping(key, value, hash, metadata);
    } finally {
      bypassEngineCommands = false;
    }
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    final ByteBuffer frsKeyBuffer = encodeKey(super.readBinaryKey(encoding), hash);
    if (removal) {
      // free the chain here if we are removing..otherwise chainFreed will be invoked from within
      chainFreed(encoding);
      frsRemove(frsKeyBuffer);
    }
    super.freeMapping(encoding, hash, removal);
  }

  @Override
  public void clear() {
    first = NULL_ENCODING;
    last = NULL_ENCODING;
    frsDelete();
    super.clear();
  }

  @Override
  public ObjectManagerEntry<I, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
    final Lock l = owner.writeLock();
    l.lock();
    long chainAddress = first;
    if (chainAddress == NULL_ENCODING) {
      l.unlock();
      return null;
    }
    try {
      LinkedChainNode node = createAtChainAddress(chainAddress);
      if (node.getLsn() >= ceilingLsn) {
        l.unlock();
        return null;
      }

      final ByteBuffer valueBuffer = super.readBinaryValue(chainAddress);
      final ByteBuffer frsKeyBuffer = encodeKey(super.readBinaryKey(chainAddress), super.readKeyHash(chainAddress));

      // WARNING: we are returning here holding a segment lock
      return compactingEntry = new SimpleObjectManagerEntry<>(identifier, frsKeyBuffer, valueBuffer, node.getLsn());
    } catch (RuntimeException | Error e) {
      l.unlock();
      throw e;
    } catch (Throwable e) {
      l.unlock();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateLsn(int hash, ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    // this call is always under a segment lock
    if (entry != compactingEntry) {
      throw new AssertionError(
          "Illegal Compaction: Tried to update the LSN on an entry that was not acquired.");
    }
    assignLsn(extractChainAddressFromValue(entry.getValue()), newLsn);
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry) {
    if (entry == null || entry != compactingEntry) {
      throw new AssertionError("Illegal Compaction: Entry to release is either null or was not acquired for compaction");
    }
    compactingEntry = null;
    owner.writeLock().unlock();
  }

  @Override
  public Long getLowestLsn() {
    Lock l = owner.readLock();
    l.lock();
    try {
      long lowest = first;
      if (lowest == NULL_ENCODING) {
        return null;
      } else {
        return createAtChainAddress(lowest).getLsn();
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long getLsn(int hash, ByteBuffer frsKeyBuffer) {
    Lock l = owner.readLock();
    l.lock();
    try {
      Long chainAddress = owner.getEncodingForHashAndBinary(hash, extractKeyFromKeyBuffer(frsKeyBuffer));
      if (chainAddress == null) {
        return null;
      } else {
        return createAtChainAddress(chainAddress).getLsn();
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public void put(int hash, ByteBuffer frsKeyBuffer, ByteBuffer valueBuffer, long lsn) {
    long chainAddress = extractChainAddressFromValue(valueBuffer);
    Lock l = owner.writeLock();
    l.lock();
    try {
      assignLsn(chainAddress, lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void replayPut(final int hash, ByteBuffer frsKeyBuffer, ByteBuffer valueBuffer, long lsn) {
    final Lock l = owner.writeLock();
    l.lock();
    try {
      long chainAddress = owner.installMappingForHashAndEncoding(hash, extractKeyFromKeyBuffer(frsKeyBuffer), valueBuffer, 0);
      final LinkedChainNode node = createAtChainAddress(chainAddress);
      linkNodeExpectingFirst(node, chainAddress, lsn);
      node.setLsn(lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void remove(int i, ByteBuffer byteBuffer) {
    // no-op as of now..
  }

  @Override
  public long size() {
    return owner.getSize();
  }

  @Override
  public long sizeInBytes() {
    return super.getOccupiedMemory();
  }

  @Override
  protected void chainFreed(final long chainAddress) {
    if (bypassEngineCommands) {
      // do not do anything when in write mapping
      return;
    }
    LinkedChainNode node = createAtChainAddress(chainAddress);
    unlinkNode(node, chainAddress);
  }

  @Override
  protected void chainAttached(long chainAddress) {
    frsPut(chainAddress);
  }

  @Override
  protected void chainModified(long chainAddress) {
    if (bypassEngineCommands) {
      return;
    }
    frsPut(chainAddress);
  }

  @Override
  protected void chainMoved(long fromChainAddress, long toChainAddress) {
    if (bypassEngineCommands) {
      return;
    }
    relinkNode(fromChainAddress, toChainAddress);
  }

  private void assignLsn(long chainAddress, long lsn) {
    LinkedChainNode node = createAtChainAddress(chainAddress);
    unlinkNode(node, chainAddress);
    linkNodeExpectingLast(node, chainAddress, lsn);
    node.setLsn(lsn);
  }

  private void relinkNode(final long fromChainAddress, final long toChainAddress) {
    LinkedChainNode fromNode = createAtChainAddress(fromChainAddress);
    LinkedChainNode toNode = createAtChainAddress(toChainAddress);

    if (toNode.getLsn() > 0) {
      // node already assigned..just unlink 'from' node and return
      unlinkNode(fromNode, fromChainAddress);
      return;
    }

    long nextChain = fromNode.getNext();
    long prevChain = fromNode.getPrevious();
    toNode.setNext(nextChain);
    toNode.setPrevious(prevChain);
    toNode.setLsn(fromNode.getLsn());

    if (last == fromChainAddress) {
      last = toChainAddress;
    }
    if (first == fromChainAddress) {
      first = toChainAddress;
    }
    if (nextChain != NULL_ENCODING) {
      LinkedChainNode next = createAtChainAddress(nextChain);
      next.setPrevious(toChainAddress);
    }
    if (prevChain != NULL_ENCODING) {
      LinkedChainNode previous = createAtChainAddress(prevChain);
      previous.setNext(toChainAddress);
    }
    fromNode.setNext(NULL_ENCODING);
    fromNode.setPrevious(NULL_ENCODING);
    fromNode.setLsn(-1);
  }

  private void unlinkNode(LinkedChainNode node, long chainAddress) {
    final long nextChain = node.getNext();
    final long prevChain = node.getPrevious();
    if (last == chainAddress) {
      last = prevChain;
    }
    if (first == chainAddress) {
      first = nextChain;
    }
    if (nextChain != NULL_ENCODING) {
      LinkedChainNode next = createAtChainAddress(nextChain);
      next.setPrevious(prevChain);
    }
    if (prevChain != NULL_ENCODING) {
      LinkedChainNode previous = createAtChainAddress(prevChain);
      previous.setNext(nextChain);
    }
    node.setNext(NULL_ENCODING);
    node.setPrevious(NULL_ENCODING);
  }

  private void linkNodeExpectingLast(LinkedChainNode node, long chainAddress, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }

    if (last == NULL_ENCODING) {
      last = chainAddress;
      first = chainAddress;
      return;
    }

    //insertion in non-empty list
    long prevChain = last;
    long nextChain;
    LinkedChainNode previous = createAtChainAddress(prevChain);
    while (true) {
      if (previous.getLsn() < lsn) {
        nextChain = previous.getNext();
        break;
      } else if (previous.getPrevious() == NULL_ENCODING) {
        nextChain = prevChain;
        prevChain = NULL_ENCODING;
        previous = null;
        break;
      }
      prevChain = previous.getPrevious();
      previous = createAtChainAddress(prevChain);
    }

    if (nextChain == NULL_ENCODING && previous != null) {
      //insertion at last
      last = chainAddress;
      node.setPrevious(prevChain);
      previous.setNext(chainAddress);
    } else {
      LinkedChainNode next = createAtChainAddress(nextChain);
      if (prevChain == NULL_ENCODING) {
        //insertion at first
        first = chainAddress;
        node.setNext(nextChain);
        next.setPrevious(chainAddress);
      } else {
        //insertion in middle
        node.setNext(nextChain);
        node.setPrevious(prevChain);
        previous.setNext(chainAddress);
        next.setPrevious(chainAddress);
      }
    }
  }

  private void linkNodeExpectingFirst(LinkedChainNode node, long chainAddress, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }
    if (last == NULL_ENCODING) {
      //insertion in empty list
      last = chainAddress;
      first = chainAddress;
      return;
    }

    //insertion in non-empty list
    long nextChain = first;
    long prevChain;
    LinkedChainNode next = createAtChainAddress(nextChain);
    while (true) {
      if (next.getLsn() > lsn) {
        prevChain = next.getPrevious();
        break;
      } else if (next.getNext() == NULL_ENCODING) {
        prevChain = nextChain;
        nextChain = NULL_ENCODING;
        next = null;
        break;
      }
      nextChain = next.getNext();
      next = createAtChainAddress(nextChain);
    }

    if (prevChain == NULL_ENCODING && next != null) {
      //insertion at first
      first = chainAddress;
      node.setNext(nextChain);
      next.setPrevious(chainAddress);
    } else {
      LinkedChainNode previous = createAtChainAddress(prevChain);
      if (nextChain == NULL_ENCODING) {
        //insertion at first
        last = chainAddress;
        node.setPrevious(prevChain);
        previous.setNext(chainAddress);
      } else {
        //insertion in middle
        node.setPrevious(prevChain);
        node.setNext(nextChain);

        next.setPrevious(chainAddress);
        previous.setNext(chainAddress);
      }
    }
  }

  private LinkedChainNode createAtChainAddress(long chainAddress) {
    return new LinkedChainNode(getExtensionHeader(chainAddress),
        getExtensionWriteContext(chainAddress));
  }

  private void frsPut(final long chainAddress) {
    try {
      beginEntityTransaction().put(identifier,
          encodeKey(super.readBinaryKey(chainAddress), super.readKeyHash(chainAddress)),
          super.readBinaryValue(chainAddress));
    } catch(TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private void frsRemove(final ByteBuffer frsKeyBuffer) {
    try {
      beginEntityTransaction().remove(identifier, frsKeyBuffer);
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private void frsDelete() {
    try {
      // this one just commit
      dataStorage.beginAutoCommitTransaction(true).delete(identifier).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private Transaction<I, ByteBuffer, ByteBuffer> beginEntityTransaction() {
    return ((ControlledTransactionRestartStore<I, ByteBuffer, ByteBuffer>) dataStorage)
        .beginEntityTransaction(synchronous, identifier);
  }
}