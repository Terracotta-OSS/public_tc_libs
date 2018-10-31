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

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.SequencedElement;
import org.ehcache.clustered.server.offheap.InternalChain;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.BinaryStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.util.Factory;

import com.terracottatech.ehcache.clustered.server.offheap.frs.caches.OffHeapSimpleCachingStrategy;
import com.terracottatech.ehcache.common.frs.ControlledTransactionRestartStore;
import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.stream.IntStream;

import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.duplicatedByteBuffer;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.encodeKey;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.encodeValue;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractEncodingFromValueBuffer;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractKeyFromKeyBuffer;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractMetadataFromValueBuffer;
import static com.terracottatech.ehcache.clustered.server.offheap.frs.RestartableKeyValueEncoder.extractValueFromValueBuffer;

/**
 * Storage engine managing hybrid pools where metadata is in offHeap and the chain data is in FRS.
 */
public class RestartableHybridChainStorageEngine<I, K> implements RestartableChainStorageEngine<I, K>, BinaryStorageEngine {
  private static final int META_LSN_OFFSET = 0;
  private static final int META_PREVIOUS_OFFSET = 8;
  private static final int META_NEXT_OFFSET = 16;
  private static final int META_KEY_HASH_OFFSET = 24;
  private static final int META_KEY_LENGTH_OFFSET = 28;
  private static final int META_DATA_SIZE_OFFSET = 32;
  private static final int META_SIZE = 36;

  private static final int META_CACHE_ADDRESS_OFFSET = 36;
  private static final int META_CACHE_SEQUENCE_OFFSET = 44;
  private static final int META_CACHE_ACCESS_COUNT_OFFSET = 52;
  private static final int META_SIZE_WITH_CACHE_META = 56;

  private static final long NULL_ENCODING = Long.MIN_VALUE;

  private static final int METADATA_SIMPLE_CHAIN = 0x1000;

  public static <I, K> Factory<? extends RestartableChainStorageEngine<I, K>> createFactory(final PageSource source,
                                                                                            final Portability<? super K> keyPortability,
                                                                                            final int minPageSize, final int maxPageSize,
                                                                                            final boolean thief, final boolean victim,
                                                                                            final I identifier, final RestartStore<I, ByteBuffer, ByteBuffer> dataStorage,
                                                                                            final boolean synchronous, final PageSource cachePageSource) {
    return (Factory<RestartableHybridChainStorageEngine<I, K>>)() ->
        new RestartableHybridChainStorageEngine<>(source, keyPortability,
            minPageSize, maxPageSize, thief, victim, identifier, dataStorage, synchronous, cachePageSource);
  }

  private final Map<Long, Map.Entry<ByteBuffer, ByteBuffer>> holdingArea = new HashMap<>();
  private final SlidingWindow<Long> removedEncodings = new SlidingWindow<>(Long.class, 128, holdingArea::remove);
  private final OffHeapStorageArea metadataArea;
  private final Portability<? super K> keyPortability;
  private final I identifier;
  private final RestartStore<I, ByteBuffer, ByteBuffer> transactionSource;
  private final boolean synchronous;
  private final CachingSupport cachingSupport;

  private volatile Owner owner;

  private long lsnFirst = NULL_ENCODING;
  private long lsnLast = NULL_ENCODING;
  private ObjectManagerEntry<I, ByteBuffer, ByteBuffer> compactingEntry;

  private volatile long dataSize = 0L;
  private long nextSequenceNumber = 0;

  public RestartableHybridChainStorageEngine(PageSource source, Portability<? super K> keyPortability, int minPageSize, int maxPageSize, boolean thief, boolean victim,
                                             I identifier, RestartStore<I, ByteBuffer, ByteBuffer> transactionSource, boolean synchronous,
                                             PageSource cachePageSource) {
    this.identifier = identifier;
    this.transactionSource = transactionSource;
    this.metadataArea = new OffHeapStorageArea(PointerSize.LONG, new MetadataOwner(), source, minPageSize, maxPageSize, thief, victim);
    this.keyPortability = keyPortability;
    this.synchronous = synchronous;
    this.cachingSupport = new CachingSupport(cachePageSource == null ? null : new OffHeapSimpleCachingStrategy(cachePageSource, minPageSize));
  }

  @Override
  public InternalChain newChain(ByteBuffer element) {
    return new GenesisLink(element);
  }

  @Override
  public InternalChain newChain(Chain chain) {
    return new GenesisLinks(chain);
  }

  @Override
  public Long writeMapping(final K key, final InternalChain value, int hash, int metadata) {
    final ByteBuffer binaryKey = keyPortability.encode(key);
    if (value instanceof GenesisLink) {
      return writeElement(hash, binaryKey, ((GenesisLink) value).element, true);
    } else if (value instanceof GenesisLinks) {
      return writeChain(hash, binaryKey, ((GenesisLinks) value).chain);
    } else {
      throw new AssertionError("only detached internal chains should be initially written");
    }
  }

  @Override
  public void attachedMapping(long encoding, int hash, int metadata) {
    Map.Entry<ByteBuffer, ByteBuffer> entry = holdingArea.get(encoding);
    if (entry == null) {
      throw new AssertionError("Entry not in holding area");
    }
    frsPut(encoding, hash, entry.getKey().duplicate(), entry.getValue().duplicate());
  }

  @Override
  public void freeMapping(long encoding, int hash, boolean removal) {
    ByteBuffer rawKey = null;
    if (removal) {
      rawKey = readBinaryKey(encoding);
    }
    cachingSupport.remove(encoding);
    unlinkNode(encoding);
    if (removal) {
      frsRemove(rawKey.duplicate(), hash);
    }
    adjustDataSizeOnRemove(encoding);
    metadataArea.free(encoding);
    removedEncodings.clearItem(encoding);
    holdingArea.remove(encoding);
  }

  @Override
  public InternalChain readValue(final long encoding) {
    return new DetachedInternalChain(encoding);
  }

  @Override
  public long size() {
    return owner.getSize();
  }

  @Override
  public Long getLowestLsn() {
    Lock l = owner.readLock();
    l.lock();
    try {
      long lowest = firstEncoding();
      if (lowest == NULL_ENCODING) {
        return null;
      } else {
        return metadataArea.readLong(lowest + META_LSN_OFFSET);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public Long getLsn(int pojoHash, ByteBuffer frsBinaryKey) {
    ByteBuffer offheapBinaryKey = extractKeyFromKeyBuffer(frsBinaryKey);
    Lock l = owner.readLock();
    l.lock();
    try {
      Long encoding = lookupEncoding(pojoHash, offheapBinaryKey);
      if (encoding == null) {
        return null;
      } else {
        return metadataArea.readLong(encoding + META_LSN_OFFSET);
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public void put(int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    //this encoding *must* be valid... this is not installed in the map yet...
    long encoding = extractEncodingFromValueBuffer(frsBinaryValue);
    Lock l = owner.writeLock();
    l.lock();
    try {
      assignLsn(encoding, lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void remove(int pojoHash, ByteBuffer frsBinaryKey) {
  }

  @Override
  public void replayPut(final int pojoHash, ByteBuffer frsBinaryKey, ByteBuffer frsBinaryValue, long lsn) {
    final ByteBuffer offheapBinaryKey = extractKeyFromKeyBuffer(frsBinaryKey);
    final ByteBuffer offheapBinaryValue = extractValueFromValueBuffer(frsBinaryValue);
    final int metadata = extractMetadataFromValueBuffer(frsBinaryValue);

    Lock l = owner.writeLock();
    l.lock();
    try {
      final long encoding = owner.installMappingForHashAndEncoding(pojoHash, offheapBinaryKey, offheapBinaryValue, metadata);
      linkNodeExpectingFirst(encoding, lsn);
      metadataArea.writeLong(encoding + META_LSN_OFFSET, lsn);
    } finally {
      l.unlock();
    }
  }

  @Override
  public ObjectManagerEntry<I, ByteBuffer, ByteBuffer> acquireCompactionEntry(long ceilingLsn) {
    Lock l = owner.writeLock();
    l.lock();
    long encoding = firstEncoding();
    if (encoding == NULL_ENCODING) {
      l.unlock();
      return null;
    }
    try {
      long lsn = metadataArea.readLong(encoding + META_LSN_OFFSET);
      if (lsn >= ceilingLsn) {
        l.unlock();
        return null;
      }

      try (Entry extractedEntry = extractEntryFromEncoding(encoding)) {
        // This exit means that the current thread will be holding the segment lock.
        return compactingEntry = new SimpleObjectManagerEntry<>(identifier,
            encodeKey(readBinaryKey(encoding), readKeyHash(encoding)),
            encodeValue(extractedEntry.getValue(), encoding, deriveMetadata(encoding)),
            lsn);
      }
    } catch (RuntimeException | Error e) {
      l.unlock();
      throw e;
    } catch (Throwable e) {
      l.unlock();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateLsn(int pojoHash, ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry, long newLsn) {
    // Expected to be called under the segment lock.
    if (entry != compactingEntry) {
      throw new IllegalArgumentException(
          "Tried to update the LSN on an entry that was not acquired.");
    }
    long encoding = extractEncodingFromValueBuffer(entry.getValue());
    assignLsn(encoding, newLsn);
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, ByteBuffer, ByteBuffer> entry) {
    if (entry == null) {
      throw new NullPointerException("Tried to release a null entry.");
    }
    if (entry != compactingEntry) {
      throw new IllegalArgumentException("Released entry is not the same as acquired entry.");
    }
    compactingEntry = null;
    // Releasing the lock acquired in acquireFirstEntry().
    owner.writeLock().unlock();
  }

  @Override
  public boolean equalsValue(Object value, final long encoding) {
    try (InternalChain chain = readValue(encoding)) {
      return chain.equals(value);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public K readKey(final long encoding, int hashCode) {
    return (K)keyPortability.decode(readBinaryKey(encoding));
  }

  @Override
  public boolean equalsKey(Object key, final long encoding) {
    return keyPortability.equals(key, readBinaryKey(encoding));
  }

  @Override
  public void clear() {
    lsnFirst = NULL_ENCODING;
    lsnLast = NULL_ENCODING;
    dataSize = 0;
    cachingSupport.clear();
    frsDelete();
    metadataArea.clear();
    holdingArea.clear();
    removedEncodings.clearAll();
  }

  @Override
  public long getAllocatedMemory() {
    return metadataArea.getAllocatedMemory() + cachingSupport.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return metadataArea.getOccupiedMemory() + cachingSupport.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return metadataArea.getOccupiedMemory();
  }

  @Override
  public long getDataSize() {
    return dataSize;
  }

  @Override
  public long sizeInBytes() {
    return metadataArea.getOccupiedMemory() + getDataSize();
  }

  @Override
  public void invalidateCache() {
    //no-op - there is no caching of portable forms yet
  }

  @Override
  public void bind(Owner owner) {
    this.owner = owner;
  }

  @Override
  public void destroy() {
    metadataArea.destroy();
  }

  @Override
  public boolean shrink() {
    return metadataArea.shrink();
  }

  @Override
  public ByteBuffer readBinaryKey(long encoding) {
    int keyLength = metadataArea.readInt(encoding + META_KEY_LENGTH_OFFSET);
    return metadataArea.readBuffer(encoding + cachingSupport.getMetaHeaderSize(), keyLength);
  }

  @Override
  public int readKeyHash(long encoding) {
    return metadataArea.readInt(encoding + META_KEY_HASH_OFFSET);
  }

  @Override
  public ByteBuffer readBinaryValue(final long encoding) {
    try (Entry extractedEntry = extractEntryFromEncoding(encoding)) {
      return extractedEntry.getDuplicatedValue();
    }
  }

  @Override
  public boolean equalsBinaryKey(ByteBuffer probeKey, long encoding) {
    return probeKey.equals(readBinaryKey(encoding));
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer binaryKey, ByteBuffer binaryValue, int pojoHash, int metadata) {
    final boolean simpleChain = (metadata & METADATA_SIMPLE_CHAIN) != 0;
    return writePartialEntry(pojoHash, binaryKey, binaryValue, simpleChain);
  }

  @Override
  public Long writeBinaryMapping(ByteBuffer[] binaryKey, ByteBuffer[] binaryValue, int pojoHash, int metadata) {
    throw new AssertionError("Operation Not supported");
  }

  private Long writeElement(int hash, ByteBuffer binaryKey, ByteBuffer binaryValue, boolean simpleChain) {
    do {
      Long result = writePartialEntry(hash, binaryKey, binaryValue, simpleChain);
      if (result != null) {
        removedEncodings.clearItem(result);
        holdingArea.put(result, new AbstractMap.SimpleImmutableEntry<>(binaryKey, binaryValue));
        return result;
      }
    } while (shrink());
    return null;
  }

  private Long writeChain(int hash, ByteBuffer binaryKey, Chain chain) {
    Iterator<Element> iterator = chain.iterator();
    HybridChain hyChain = new HybridChain(iterator.next().getPayload(), METADATA_SIMPLE_CHAIN);
    while (iterator.hasNext()) {
      hyChain.append(iterator.next().getPayload());
    }
    return writeElement(hash, binaryKey, hyChain.getValue(), false);
  }

  private Entry extractEntryFromEncoding(long encoding) {
    Map.Entry<ByteBuffer, ByteBuffer> entry = holdingArea.get(encoding);
    if (entry != null) {
      return new CachedEntry(entry.getValue());
    } else {
      ByteBuffer cachedValue = cachingSupport.get(encoding);
      if (cachedValue == null) {
        long lsn = metadataArea.readLong(encoding + META_LSN_OFFSET);
        return new FRSEntry<>(transactionSource.get(lsn));
      } else {
        return new CachedEntry(cachedValue);
      }
    }
  }

  private int deriveMetadata(long encoding) {
    int metadata = 0;
    if (metadataArea.readInt(encoding + META_DATA_SIZE_OFFSET) < 0) {
      metadata |= METADATA_SIMPLE_CHAIN;
    }
    return metadata;
  }

  private int getActualEntrySize(final long encoding) {
    return cachingSupport.getMetaHeaderSize() + metadataArea.readInt(encoding + META_KEY_LENGTH_OFFSET);
  }

  private void frsPut(long encoding, int hash, ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
    try {
      cachingSupport.updateSequence(encoding);
      beginEntityTransaction().put(identifier, encodeKey(keyBuffer, hash),
          encodeValue(valueBuffer, encoding, deriveMetadata(encoding)));
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private void frsDelete() {
    try {
      transactionSource.beginAutoCommitTransaction(synchronous).delete(identifier).commit();
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private void frsRemove(ByteBuffer keyBuffer, int hash) {
    ByteBuffer frsBinaryKey = encodeKey(keyBuffer, hash);
    try {
      beginEntityTransaction().remove(identifier, frsBinaryKey);
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    }
  }

  private Transaction<I, ByteBuffer, ByteBuffer> beginEntityTransaction() {
    return ((ControlledTransactionRestartStore<I, ByteBuffer, ByteBuffer>) transactionSource)
        .beginEntityTransaction(synchronous, identifier);
  }

  private Long writePartialEntry(int hash, ByteBuffer binaryKey, ByteBuffer binaryValue, boolean singleChain) {
    int entrySize = cachingSupport.getMetaHeaderSize() + binaryKey.remaining();

    long encoding = metadataArea.allocate(entrySize);
    if (encoding >= 0) {
      int size = binaryKey.remaining() + binaryValue.remaining();
      metadataArea.writeLong(encoding + META_LSN_OFFSET, NULL_ENCODING);
      metadataArea.writeLong(encoding + META_NEXT_OFFSET, NULL_ENCODING);
      metadataArea.writeLong(encoding + META_PREVIOUS_OFFSET, NULL_ENCODING);
      metadataArea.writeInt(encoding + META_KEY_HASH_OFFSET, hash);
      writeDataSize(encoding, size, singleChain);
      cachingSupport.initCacheMeta(encoding);
      writeBinaryKey(encoding, binaryKey);
      return encoding;
    } else {
      return null;
    }
  }

  private void writeDataSize(long encoding, int size, boolean simpleChain) {
    metadataArea.writeInt(encoding + META_DATA_SIZE_OFFSET, (simpleChain) ? Integer.MIN_VALUE | size : size);
    dataSize += size;
  }

  private int readDataSize(long encoding) {
    return Integer.MAX_VALUE & metadataArea.readInt(encoding + META_DATA_SIZE_OFFSET);
  }

  private void adjustDataSizeOnRemove(long encoding) {
    dataSize -= readDataSize(encoding);
  }

  private void writeBinaryKey(long encoding, ByteBuffer binaryKey) {
    metadataArea.writeInt(encoding + META_KEY_LENGTH_OFFSET, binaryKey.remaining());
    metadataArea.writeBuffer(encoding + cachingSupport.getMetaHeaderSize(), binaryKey.duplicate());
  }

  private void assignLsn(long encoding, long lsn) {
    unlinkNode(encoding);
    linkNodeExpectingLast(encoding, lsn);
    metadataArea.writeLong(encoding + META_LSN_OFFSET, lsn);
    removedEncodings.putItem(encoding);
  }

  private void unlinkNode(long encoding) {
    long next = metadataArea.readLong(encoding + META_NEXT_OFFSET);
    long prev = metadataArea.readLong(encoding + META_PREVIOUS_OFFSET);
    if (lsnLast == encoding) {
      lsnLast = prev;
    }
    if (lsnFirst == encoding) {
      lsnFirst = next;
    }
    if (next != NULL_ENCODING) {
      metadataArea.writeLong(next + META_PREVIOUS_OFFSET, prev);
    }
    if (prev != NULL_ENCODING) {
      metadataArea.writeLong(prev + META_NEXT_OFFSET, next);
    }

    metadataArea.writeLong(encoding + META_NEXT_OFFSET, NULL_ENCODING);
    metadataArea.writeLong(encoding + META_PREVIOUS_OFFSET, NULL_ENCODING);
  }

  private void linkNodeExpectingLast(long node, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }

    if (lsnLast == NULL_ENCODING) {
      lsnLast = node;
      lsnFirst = node;
      return;
    }

    //insertion in non-empty list
    long previous = lsnLast;
    long next;
    while (true) {
      if (metadataArea.readLong(previous + META_LSN_OFFSET) < lsn) {
        next = metadataArea.readLong(previous + META_NEXT_OFFSET);
        break;
      } else if (metadataArea.readLong(previous + META_PREVIOUS_OFFSET) == NULL_ENCODING) {
        next = previous;
        previous = NULL_ENCODING;
        break;
      }
      previous = metadataArea.readLong(previous + META_PREVIOUS_OFFSET);
    }

    if (next == NULL_ENCODING) {
      lsnLast = node;
      metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
      metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
    } else {
      if (previous == NULL_ENCODING) {
        lsnFirst = node;
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);
        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
      } else {
        //insertion in middle
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);

        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
      }
    }
  }

  private void linkNodeExpectingFirst(long node, long lsn) {
    if (lsn < 0) {
      throw new AssertionError("Received illegal lsn " + lsn);
    }
    if (lsnLast == NULL_ENCODING) {
      lsnLast = node;
      lsnFirst = node;
      return;
    }
    //insertion in non-empty list
    long next = lsnFirst;
    long previous;
    while (true) {
      if (metadataArea.readLong(next + META_LSN_OFFSET) > lsn) {
        previous = metadataArea.readLong(next + META_PREVIOUS_OFFSET);
        break;
      } else if (metadataArea.readLong(next + META_NEXT_OFFSET) == NULL_ENCODING) {
        previous = next;
        next = NULL_ENCODING;
        break;
      }
      next = metadataArea.readLong(next + META_NEXT_OFFSET);
    }
    if (previous == NULL_ENCODING) {
      lsnFirst = node;
      metadataArea.writeLong(node + META_NEXT_OFFSET, next);
      metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
    } else {
      if (next == NULL_ENCODING) {
        lsnLast = node;
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
      } else {
        //insertion in middle
        metadataArea.writeLong(node + META_PREVIOUS_OFFSET, previous);
        metadataArea.writeLong(node + META_NEXT_OFFSET, next);

        metadataArea.writeLong(next + META_PREVIOUS_OFFSET, node);
        metadataArea.writeLong(previous + META_NEXT_OFFSET, node);
      }
    }
  }

  private long firstEncoding() {
    return lsnFirst;
  }

  private Long lookupEncoding(int hash, ByteBuffer offHeapBinaryKey) {
    return owner.getEncodingForHashAndBinary(hash, offHeapBinaryKey);
  }

  private class MetadataOwner implements OffHeapStorageArea.Owner {

    @Override
    public boolean evictAtAddress(long address, boolean shrink) {
      int hash = readKeyHash(address);
      int slot = owner.getSlotForHashAndEncoding(hash, address, ~0);
      return owner.evict(slot, shrink);
    }

    @Override
    public Lock writeLock() {
      return owner.writeLock();
    }

    @Override
    public boolean isThief() {
      return owner.isThiefForTableAllocations();
    }

    @Override
    public boolean moved(long from, long to) {
      removedEncodings.clearItem(from);
      if (owner.updateEncoding(readKeyHash(to), from, to, ~0)) {
        if (lsnLast == from) {
          lsnLast = to;
        }
        if (lsnFirst == from) {
          lsnFirst = to;
        }
        long next = metadataArea.readLong(to + META_NEXT_OFFSET);
        if (next != NULL_ENCODING) {
          metadataArea.writeLong(next + META_PREVIOUS_OFFSET, to);
        }
        long prev = metadataArea.readLong(to + META_PREVIOUS_OFFSET);
        if (prev != NULL_ENCODING) {
          metadataArea.writeLong(prev + META_NEXT_OFFSET, to);
        }
        return true;
      } else {
        return false;
      }
    }

    @Override
    public int sizeOf(long address) {
      return getActualEntrySize(address);
    }
  }

  private final class CachingSupport implements OffHeapByteBufferCache.EventListener {
    private final OffHeapByteBufferCache cacheStrategy;

    private CachingSupport(OffHeapByteBufferCache cacheStrategy) {
      this.cacheStrategy = cacheStrategy;
      if (this.cacheStrategy != null) {
        cacheStrategy.setEventListener(this);
      }
    }

    private int getMetaHeaderSize() {
      return (cacheStrategy == null) ? META_SIZE : META_SIZE_WITH_CACHE_META;
    }

    private void initCacheMeta(long metaAddress) {
      if (cacheStrategy != null) {
        metadataArea.writeLong(metaAddress + META_CACHE_ADDRESS_OFFSET, NULL_ENCODING);
        metadataArea.writeInt(metaAddress + META_CACHE_ACCESS_COUNT_OFFSET, 1);
        metadataArea.writeLong(metaAddress + META_CACHE_SEQUENCE_OFFSET, 1L);
      }
    }

    private void remove(long metaAddress) {
      if (cacheStrategy != null) {
        final long cacheAddress = metadataArea.readLong(metaAddress + META_CACHE_ADDRESS_OFFSET);
        if (cacheAddress != NULL_ENCODING) {
          cacheStrategy.remove(cacheAddress, metaAddress);
          metadataArea.writeLong(metaAddress + META_CACHE_ADDRESS_OFFSET, NULL_ENCODING);
        }
      }
    }

    private ByteBuffer get(long metaAddress) {
      if (cacheStrategy != null) {
        final long cacheAddress = metadataArea.readLong(metaAddress + META_CACHE_ADDRESS_OFFSET);
        final int accessCount = metadataArea.readInt(metaAddress + META_CACHE_ACCESS_COUNT_OFFSET);
        metadataArea.writeInt(metaAddress + META_CACHE_ACCESS_COUNT_OFFSET, accessCount + 1);
        return (cacheAddress != NULL_ENCODING) ? cacheStrategy.get(cacheAddress, metaAddress) : null;
      }
      return null;
    }

    @Override
    public void onEviction(long metaAddress) {
      if (cacheStrategy != null) {
        metadataArea.writeLong(metaAddress + META_CACHE_ADDRESS_OFFSET, NULL_ENCODING);
      }
    }

    @Override
    public void onMove(long metaAddress, long fromCacheAddress, long toCacheAddress) {
      if (cacheStrategy != null) {
        metadataArea.writeLong(metaAddress + META_CACHE_ADDRESS_OFFSET, toCacheAddress);
      }
    }

    private void conditionalPut(long metaAddress, long currentSequence, ByteBuffer unmodifiedValue) {
      if (cacheStrategy != null && currentSequence >= 0) {
        final long cacheAddress = metadataArea.readLong(metaAddress + META_CACHE_ADDRESS_OFFSET);
        final int accessCount = metadataArea.readInt(metaAddress + META_CACHE_ACCESS_COUNT_OFFSET);
        if (cacheAddress == NULL_ENCODING) {
          final long latestSequence = metadataArea.readLong(metaAddress + META_CACHE_SEQUENCE_OFFSET);
          if (currentSequence >= latestSequence) {
            // no changes to data..so this can be cached
            Long address = cacheStrategy.put(metaAddress, unmodifiedValue, accessCount);
            if (address != null) {
              metadataArea.writeLong(metaAddress + META_CACHE_ADDRESS_OFFSET, address);
            }
          }
        }
      }
    }

    private void clear() {
      if (cacheStrategy != null) {
        cacheStrategy.clear();
      }
    }

    private long getSequence(long metaAddress) {
      if (cacheStrategy != null) {
        return metadataArea.readLong(metaAddress + META_CACHE_SEQUENCE_OFFSET);
      }
      return Long.MIN_VALUE;
    }

    private void updateSequence(long metaAddress) {
      if (cacheStrategy != null) {
        final long newSequence = getSequence(metaAddress) + 1;
        metadataArea.writeLong(metaAddress + META_CACHE_SEQUENCE_OFFSET, newSequence);
      }
    }

    private long getAllocatedMemory() {
      return (cacheStrategy == null) ? 0 : cacheStrategy.getAllocatedMemory();
    }

    public long getOccupiedMemory() {
      return (cacheStrategy == null) ? 0 : cacheStrategy.getOccupiedMemory();
    }
  }

  /**
   * Represents the initial form of a chain before the storage engine writes the chain mapping
   * to the underlying map against the key.
   */
  private static abstract class GenesisChain implements InternalChain {
    @Override
    public Chain detach() {
      throw new AssertionError("Chain not in storage yet. Cannot be detached");
    }

    @Override
    public boolean append(ByteBuffer element) {
      throw new AssertionError("Chain not in storage yet. Cannot be appended");
    }

    @Override
    public boolean replace(Chain expected, Chain replacement) {
      throw new AssertionError("Chain not in storage yet. Cannot be mutated");
    }

    @Override
    public void close() {
      //no-op
    }
  }

  /**
   * Represents a simple {@link GenesisChain} that contains a single link.
   */
  private static class GenesisLink extends GenesisChain {
    private final ByteBuffer element;

    GenesisLink(ByteBuffer buffer) {
      element = buffer;
    }
  }

  /**
   * Represents a more complex {@link GenesisChain} that contains multiple links represented itself
   * as a {@link Chain}
   */
  private static class GenesisLinks extends GenesisChain {
    private final Chain chain;

    GenesisLinks(Chain chain) {
      this.chain = chain;
    }
  }

  private final class HybridChain implements Chain {
    private static final int ELEMENT_HEADER_SIZE = 12;

    private final Deque<Element> elements;
    private volatile boolean modified;

    private HybridChain(ByteBuffer value, int metadata) {
      this.elements = new ArrayDeque<>();
      final boolean simpleChain = (metadata & METADATA_SIMPLE_CHAIN) != 0;
      if (simpleChain) {
        // first instance of the chain..so not yet sequenced
        this.elements.addLast(value::asReadOnlyBuffer);
      } else {
        while (value.hasRemaining()) {
          final int currentPosition = value.position();
          final long elementSequence = value.getLong();
          final int elementLength = value.getInt();
          final ByteBuffer element = value.slice();
          element.limit(elementLength);
          value.position(currentPosition + ELEMENT_HEADER_SIZE + elementLength);
          this.elements.addLast(sequencedElement(element, elementSequence));
        }
      }
      this.modified = false;
    }

    public Deque<Element> detach() {
      Deque<Element> toRet = new ArrayDeque<>(elements.size());
      elements.forEach((e) -> {
        if (e instanceof SequencedElement) {
          toRet.addLast(new DetachedSequencedElement((SequencedElement)e));
        } else {
          toRet.addLast(new DetachedElement(e));
        }
      });
      return toRet;
    }

    @SuppressWarnings({ "cast", "RedundantCast" })
    private ByteBuffer getValue() {
      int totalSize = 0;
      for (Element e : elements) {
        totalSize += ELEMENT_HEADER_SIZE + e.getPayload().remaining();
      }
      final ByteBuffer valueBuffer = ByteBuffer.allocate(totalSize);
      elements.forEach((e) -> {
        if (e instanceof SequencedElement) {
          valueBuffer.putLong(((SequencedElement)e).getSequenceNumber());
        } else {
          valueBuffer.putLong(0L);
        }
        valueBuffer.putInt(e.getPayload().remaining());
        valueBuffer.put(e.getPayload());
      });
      return (ByteBuffer)valueBuffer.flip();        // made redundant in Java 9/10
    }

    private void append(ByteBuffer element) {
      this.elements.addLast(newElement(element));
      this.modified = true;
    }

    private Element sequencedElement(final ByteBuffer element, final long sequence) {
      return new SequencedElement() {
        @Override
        public long getSequenceNumber() {
          return sequence;
        }

        @Override
        public ByteBuffer getPayload() {
          return element.asReadOnlyBuffer();
        }
      };
    }

    private Element newElement(ByteBuffer element) {
      return new SequencedElement() {
        @Override
        public long getSequenceNumber() {
          return nextSequenceNumber++;
        }

        @Override
        public ByteBuffer getPayload() {
          return element.asReadOnlyBuffer();
        }
      };
    }

    private boolean replace(Chain expected, Chain replacement) {
      if (expected.isEmpty()) {
        throw new IllegalArgumentException("Empty expected sequence");
      } else if (replacement.isEmpty()) {
        removeLinks(expected);
        return true;
      } else {
        return replaceLinks(expected, replacement);
      }
    }

    private boolean removeLinks(Chain expected) {
      final int n = matchingPart(expected);
      if (n == 0) {
        return false;
      }
      this.modified = true;
      IntStream.range(0, n).forEach((i) -> elements.removeFirst());
      return true;
    }

    private boolean replaceLinks(Chain expected, Chain replacement) {
      if (removeLinks(expected)) {
        Iterator<Element> replacementIter = replacement.reverseIterator();
        do {
          this.elements.addFirst(replacementIter.next());
        } while (replacementIter.hasNext());
      }
      return true;
    }

    private int matchingPart(Chain expected) {
      Iterator<Element> actualIter = this.iterator();
      Iterator<Element> expectedIter = expected.iterator();
      int n = 0;
      do {
        if (actualIter.hasNext()) {
          if (!compare(actualIter.next(), expectedIter.next())) {
            n = 0;
            break;
          }
        } else {
          n = 0;
          break;
        }
        n++;
      } while (expectedIter.hasNext());
      return n;
    }

    private boolean compare(Element actual, Element expected) {
      if (expected instanceof SequencedElement) {
        if (((SequencedElement)expected).getSequenceNumber() > 0) {
          return actual instanceof SequencedElement && ((SequencedElement)actual).getSequenceNumber() ==
                                                       ((SequencedElement)expected).getSequenceNumber();
        } else {
          return actual.getPayload().equals(expected.getPayload());
        }
      } else {
        return actual.getPayload().equals(expected.getPayload());
      }
    }

    @Override
    public Iterator<Element> reverseIterator() {
      return elements.descendingIterator();
    }

    @Override
    public boolean isEmpty() {
      return elements.isEmpty();
    }

    @Override
    public Iterator<Element> iterator() {
      return elements.iterator();
    }

    @Override
    public int length() {
      return elements.size();
    }
  }

  private final class DetachedInternalChain implements InternalChain {
    private final long encoding;
    private final HybridChain hyChain;
    private final long currentSequence;
    private final Entry currentEntry;

    public DetachedInternalChain(long encoding) {
      this.encoding = encoding;
      this.currentEntry = extractEntryFromEncoding(encoding);
      this.hyChain = new HybridChain(this.currentEntry.getValue().duplicate(), deriveMetadata(encoding));
      this.currentSequence = cachingSupport.getSequence(encoding);
    }

    @Override
    public Chain detach() {
      return new Chain() {
        private final Deque<Element> elements = hyChain.detach();
        @Override
        public Iterator<Element> reverseIterator() {
          return elements.descendingIterator();
        }

        @Override
        public boolean isEmpty() {
          return elements.isEmpty();
        }

        @Override
        public Iterator<Element> iterator() {
          return elements.iterator();
        }

        @Override
        public int length() {
          return elements.size();
        }
      };
    }

    @Override
    public boolean append(ByteBuffer element) {
      hyChain.append(element);
      return true;
    }

    @Override
    public boolean replace(Chain expected, Chain replacement) {
      return hyChain.replace(expected, replacement);
    }

    @Override
    public void close() {
      if (hyChain.modified) {
        removedEncodings.clearItem(encoding);
        cachingSupport.remove(encoding);
        final int hash = readKeyHash(encoding);
        if (hyChain.elements.isEmpty()) {
          // free chain
          Integer slot = owner.getSlotForHashAndEncoding(hash, encoding, ~0);
          if (slot == null || !owner.evict(slot, true)) {
            throw new AssertionError("Unexpected failure to evict slot " + slot);
          }
        } else {
          // update chain..no need to change offheap location as chain is in frs only
          final ByteBuffer binaryKey = readBinaryKey(encoding);
          final ByteBuffer binaryValue = hyChain.getValue();
          adjustDataSizeOnRemove(encoding);
          int size = binaryKey.remaining() + binaryValue.remaining();
          writeDataSize(encoding, size, false);
          holdingArea.put(encoding, new AbstractMap.SimpleImmutableEntry<>(binaryKey, binaryValue));
          frsPut(encoding, hash, binaryKey.duplicate(), binaryValue.duplicate());
        }
      } else {
        cachingSupport.conditionalPut(encoding, currentSequence, currentEntry.getValue());
      }
      currentEntry.close();
    }
  }

  private static class DetachedElement implements Element {
    private final ByteBuffer detachedPayload;

    private DetachedElement(Element attached) {
      this.detachedPayload = duplicatedByteBuffer(attached.getPayload(), false);
    }

    @Override
    public ByteBuffer getPayload() {
      return detachedPayload;
    }
  }

  private static final class DetachedSequencedElement extends DetachedElement implements SequencedElement {
    private final long sequenceNumber;

    public DetachedSequencedElement(SequencedElement attached) {
      super(attached);
      sequenceNumber = attached.getSequenceNumber();
    }

    @Override
    public long getSequenceNumber() {
      return sequenceNumber;
    }
  }

  private interface Entry extends AutoCloseable {
    ByteBuffer getValue();
    ByteBuffer getDuplicatedValue();
    void close();
  }

  private static class CachedEntry implements Entry {
    private final ByteBuffer value;

    public CachedEntry(ByteBuffer value) {
      this.value = value.duplicate();
    }

    @Override
    public ByteBuffer getValue() {
      return value;
    }

    @Override
    public ByteBuffer getDuplicatedValue() {
      // no need to duplicate cached value
      return value;
    }

    @Override
    public void close() {
      // noop
    }
  }

  private static class FRSEntry<I> implements Entry {
    private final Tuple<I, ByteBuffer, ByteBuffer> frsEntry;
    private final ByteBuffer extractedValue;

    public FRSEntry(Tuple<I, ByteBuffer, ByteBuffer> frsEntry) {
      this.frsEntry = frsEntry;
      this.extractedValue = extractValueFromValueBuffer(frsEntry.getValue());
    }

    @Override
    public ByteBuffer getValue() {
      return extractedValue;
    }

    @Override
    public ByteBuffer getDuplicatedValue() {
      return duplicatedByteBuffer(extractedValue);
    }

    @Override
    public void close() {
      if (frsEntry instanceof Disposable) {
        ((Disposable)frsEntry).dispose();
      }
    }
  }
}
