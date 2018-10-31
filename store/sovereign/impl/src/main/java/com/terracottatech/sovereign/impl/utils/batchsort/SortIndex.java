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

import java.util.ArrayList;
import java.util.stream.LongStream;

public interface SortIndex {

  long addressOf(long index);

  void add(long address);

  void swap(long index1, long index2);

  void clear();

  long size();

  LongStream inOrderStream();

  void dispose();

  SortIndex duplicate();

  void set(long index, long address);

  class Naive implements SortIndex {
    private ArrayList<Long> addresses = new ArrayList<>();

    @Override
    public long addressOf(long index) {
      return addresses.get((int) index);
    }

    @Override
    public void add(long address) {
      addresses.add(address);
    }

    @Override
    public void swap(long index1, long index2) {
      Long tmp = addresses.get((int) index1);
      addresses.set((int) index1, addresses.get((int) index2));
      addresses.set((int) index2, tmp);
    }

    @Override
    public void clear() {
      addresses.clear();
    }

    @Override
    public long size() {
      return addresses.size();
    }

    @Override
    public LongStream inOrderStream() {
      return addresses.stream().mapToLong(value -> value);
    }

    @Override
    public void dispose() {
      addresses.clear();
    }

    @Override
    public SortIndex duplicate() {
      Naive tmp = new Naive();
      tmp.addresses.addAll(addresses);
      return tmp;
    }

    @Override
    public void set(long index, long address) {
      addresses.set((int) index, address);
    }

  }
}
