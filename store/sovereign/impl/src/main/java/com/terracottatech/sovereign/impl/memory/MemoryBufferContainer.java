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

import com.terracottatech.sovereign.impl.model.SovereignBufferContainer;
import com.terracottatech.sovereign.impl.model.SovereignShardObject;
import com.terracottatech.sovereign.spi.store.Context;
import com.terracottatech.sovereign.spi.store.Locator;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.PointerSize;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.terracottatech.sovereign.impl.SovereignAllocationResource.Type.AddressList;

/**
 * @author cschanck
 *         TODO to "de-race" access to live and source per cljo
 **/
public class MemoryBufferContainer implements SovereignBufferContainer, SovereignShardObject {

  private final SovereignRuntime<?> runtime;
  private final int shardIndex;
  private final boolean strategyStealsBuffer;
  private OffHeapStorageArea source;
  private MemoryAddressList live;
  private volatile boolean disposed = false;

  protected MemoryBufferContainer(ShardSpec shardSpec, SovereignRuntime<?> runtime, PageSource pageSource) {
    this.runtime = runtime;
    this.strategyStealsBuffer = runtime.getBufferStrategy().fromConsumesByteBuffer();
    this.shardIndex = shardSpec.getShardIndex();
    source = new OffHeapStorageArea(PointerSize.LONG, null, pageSource, 4 * 1024, 8 * 1024 * 1024, false, false);
    live = new MemoryAddressList(runtime.allocator().getBufferAllocator(AddressList),
                                 this::free,
                                 1000,
                                 runtime.getShardEngine().addShardLambdaFor(shardIndex),
                                 runtime.getShardEngine().removeShardIndexLambda());
  }

  public MemoryAddressList getLive() {
    this.testDisposed();
    return live;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  long getUsed() {
    this.testDisposed();
    return (live.size() * Long.BYTES) + source.getOccupiedMemory();
  }

  long getReserved() {
    this.testDisposed();
    return (live.getAllocatedSlots() * Long.BYTES) + source.getAllocatedMemory();
  }

  protected final void testDisposed() throws IllegalStateException {
    if (this.disposed) {
      throw new IllegalStateException("Attempt to use disposed buffer container");
    }
  }

  @Override
  public SovereignRuntime<?> runtime() {
    return runtime;
  }

  @Override
  public PersistentMemoryLocator add(ByteBuffer data) {
    this.testDisposed();
    PersistentMemoryLocator ret = new PersistentMemoryLocator(live(store(data)), MemoryLocatorFactory.NONE);
    return ret;
  }

  @Override
  public PersistentMemoryLocator reinstall(long lsn, long persistentKey, ByteBuffer data) {
    long storageAddr = store(data);
    getLive().reinstall(persistentKey, storageAddr);
    PersistentMemoryLocator loc = new PersistentMemoryLocator(persistentKey, null);
    return loc;
  }

  @Override
  public PersistentMemoryLocator restore(long key, ByteBuffer data, ByteBuffer oldData) {
    long storageAddr = store(data);
    getLive().restore(key, storageAddr);
    return new PersistentMemoryLocator(key, null);
  }

  @Override
  public void deleteIfPresent(long key) {
    if (getLive().contains(key)) {
      this.delete(new PersistentMemoryLocator(key, null));
    }
  }

  @Override
  public boolean delete(PersistentMemoryLocator key) {
    this.testDisposed();
    ByteBuffer prior = get(key);
    if (prior != null) {
      dead(key.index());
    }

    return true;
  }

  @Override
  public PersistentMemoryLocator replace(PersistentMemoryLocator priorKey, ByteBuffer data) {
    this.testDisposed();
    if (priorKey == null) {
      // devolve to add.
      return add(data);
    } else {
      PersistentMemoryLocator ret = new PersistentMemoryLocator(trade(priorKey.index(), store(data)),
                                                                MemoryLocatorFactory.NONE);
      return ret;
    }
  }

  @Override
  public ByteBuffer get(PersistentMemoryLocator key) {
    this.testDisposed();
    if (!key.isValid()) {
      return null;
    }
    long address = getLive().get(key.index());
    if (address <= 0L) {
      return null;
    }
    return materialize(address);
  }

  public ByteBuffer getForSlot(long slot) {
    this.testDisposed();
    long address = getLive().get(slot);
    if (address <= 0L) {
      return null;
    }
    return materialize(address);
  }

  protected ByteBuffer materialize(long address) {
    int len = source.readInt(address);
    if (strategyStealsBuffer) {
      ByteBuffer[] tmp = source.readBuffers(address + 4, len);
      int sz = 0;
      for (ByteBuffer b : tmp) {
        sz = sz + b.remaining();
      }
      ByteBuffer ret = ByteBuffer.allocate(sz);
      for (ByteBuffer b : tmp) {
        ret.put(b);
      }
      ret.clear();
      return ret.asReadOnlyBuffer();
    } else {
      return source.readBuffer(address + 4, len);
    }
  }

  private class TraversalContext implements MemoryLocatorFactory {
    private final Iterator<Long> liveList;

    public TraversalContext(Context context) {
      this.liveList = live.iterator(context);
    }

    @Override
    public PersistentMemoryLocator createNext() {
      testDisposed();
      if (liveList.hasNext()) {
        return new PersistentMemoryLocator(liveList.next(), this);
      }
      return PersistentMemoryLocator.INVALID;
    }

    @Override
    public PersistentMemoryLocator createPrevious() {
      return PersistentMemoryLocator.INVALID;
    }

    @Override
    public Locator.TraversalDirection direction() {
      return Locator.TraversalDirection.FORWARD;
    }

    public PersistentMemoryLocator allocateFirstLocator() {
      testDisposed();
      if (liveList.hasNext()) {
        return new PersistentMemoryLocator(liveList.next(), this);
      } else {
        return PersistentMemoryLocator.INVALID;
      }
    }
  }

  @Override
  public PersistentMemoryLocator first(ContextImpl context) {
    this.testDisposed();
    return new TraversalContext(context).allocateFirstLocator();
  }

  @Override
  public void dispose() {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.live.dispose();
    this.live = null;

    this.source.destroy();
    this.source = null;

  }

  @Override
  public long count() {
    this.testDisposed();
    return live.size();
  }

  protected long store(ByteBuffer raw) {
    long address = -1l;

    this.testDisposed();
    address = source.allocate(raw.remaining() + Integer.BYTES);
    if (address == 0) {
      throw new AssertionError("zero address");
    }
    if (address < 0) {
      throw new OutOfMemoryError();
    }

    source.writeInt(address, raw.remaining());
    source.writeBuffer(address + 4, raw.slice());
    return address;

  }

  protected long live(long add) {
    this.testDisposed();
    return live.put(add);
  }

  protected long trade(long index, long add) {
    this.testDisposed();
    live.tradeInPlace(index, add);
    return index;
  }

  protected void dead(long index) {
    this.testDisposed();
    live.clear(index);
  }

  protected void free(long uid) {
    this.testDisposed();
    source.free(uid);
  }

  public void finishRestart() {
    getLive().finishInitialization();
  }

}
