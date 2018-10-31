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
import java.util.AbstractMap;
import java.util.ArrayList;

public interface SortScratchPad<V> {

  long ingest(ByteBuffer k, V v) throws IOException;

  AbstractMap.Entry<ByteBuffer, V> fetchKV(long addr) throws IOException;

  ByteBuffer fetchK(long addr) throws IOException;

  long count();

  void clear() throws IOException;

  void dispose() throws IOException;

  class Naive<VV> implements SortScratchPad<VV> {
    private ArrayList<AbstractMap.Entry<ByteBuffer, VV>> list = new ArrayList<>();

    @Override
    public long ingest(ByteBuffer k, VV v) {
      list.add(new AbstractMap.SimpleEntry<ByteBuffer, VV>(k, v));
      return list.size() - 1;
    }

    @Override
    public AbstractMap.Entry<ByteBuffer, VV> fetchKV(long addr) {
      return list.get((int) addr);
    }

    @Override
    public ByteBuffer fetchK(long addr) {
      return fetchKV(addr).getKey();
    }

    @Override
    public long count() {
      return list.size();
    }

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public void dispose() {
      list.clear();
    }
  }
}
