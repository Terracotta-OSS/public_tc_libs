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
package com.terracottatech.sovereign.impl.utils.batchsort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Stream;

public interface SortArea<V> {
  static <VV> SortArea<VV> areaOf(SortIndex sortIndex, SortScratchPad<VV> scratch) {
    return new SortAreaImpl<VV>(sortIndex, scratch);
  }

  static <VV> SortArea<VV> naive() {
    return areaOf(new SortIndex.Naive(), new SortScratchPad.Naive<VV>());
  }

  void dispose() throws IOException;

  long size();

  void clear() throws IOException;

  void swap(long index1, long index2);

  void ingest(ByteBuffer k, V v) throws IOException;

  ByteBuffer fetchK(long index) throws IOException;

  Stream<Map.Entry<ByteBuffer, V>> asStream();

  SortIndex getIndex();

  SortIndex setIndex(SortIndex newIndex);

  class SortAreaImpl<VV> implements SortArea<VV> {
    private final SortScratchPad<VV> scratch;
    private SortIndex sortIndex;

    public SortAreaImpl(SortIndex index, SortScratchPad<VV> scratch) {
      this.sortIndex = index;
      this.scratch = scratch;
    }

    @Override
    public void dispose() throws IOException {
      sortIndex.dispose();
      scratch.dispose();
    }

    @Override
    public long size() {
      return sortIndex.size();
    }

    @Override
    public void clear() throws IOException {
      sortIndex.clear();
      scratch.clear();
    }

    @Override
    public void swap(long index1, long index2) {
      sortIndex.swap(index1, index2);
    }

    @Override
    public void ingest(ByteBuffer k, VV v) throws IOException {
      long addr = scratch.ingest(k, v);
      sortIndex.add(addr);
    }

    Map.Entry<ByteBuffer, VV> fetchKV(long index) throws IOException {
      long addr = sortIndex.addressOf(index);
      return scratch.fetchKV(addr);
    }

    @Override
    public ByteBuffer fetchK(long index) throws IOException {
      long addr = sortIndex.addressOf(index);
      return scratch.fetchK(addr);
    }

    @Override
    public Stream<Map.Entry<ByteBuffer, VV>> asStream() {
      return sortIndex.inOrderStream().mapToObj(value -> {
        try {
          return scratch.fetchKV(value);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    @Override
    public SortIndex getIndex() {
      return sortIndex;
    }

    @Override
    public SortIndex setIndex(SortIndex newIndex) {
      SortIndex old = sortIndex;
      sortIndex=newIndex;
      return old;
    }
  }
}
