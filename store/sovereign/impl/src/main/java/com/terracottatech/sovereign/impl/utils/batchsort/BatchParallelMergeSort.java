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
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public class BatchParallelMergeSort implements Sorter {
  public static final int QSORT_SIZE = 128;
  private final int par;

  public BatchParallelMergeSort(int par) {
    this.par = Math.max(par, 1);
  }

  public BatchParallelMergeSort() {
    this(Runtime.getRuntime().availableProcessors() / 2);
  }

  @Override
  public void sort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    mergeSort(area, comparator, low, high);
  }

  private void mergeSort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    high = Math.min(high, area.size() - 1);
    // Each 1-element run in A is already "sorted".
    // Make successively longer sorted runs of length 2, 4, 8, 16... until whole array is sorted.
    SortIndex arrayA = area.getIndex();
    SortIndex arrayB = arrayA.duplicate();
    long size = high - low + 1;
    ExecutorService pool = Executors.newFixedThreadPool(par);

    firstPass(pool, comparator, area, low, high, QSORT_SIZE);

    for (long width = QSORT_SIZE; width < size; width = 2 * width) {
      mergePass(pool, area, comparator, arrayA, arrayB, low, size, width);

      area.setIndex(arrayB);
      arrayB = arrayA;
      arrayA = area.getIndex();
    }
    pool.shutdownNow();
    try {
      pool.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
    }
  }

  private void firstPass(ExecutorService pool,
                         Comparator<ByteBuffer> comparator,
                         SortArea<?> area,
                         long low,
                         long high,
                         int width) throws IOException {
    final LongSupplier next = new LongSupplier() {
      long i = low;

      @Override
      public synchronized long getAsLong() {
        if (i > high) {
          return -1l;
        }
        long ret = i;
        i = i + width;
        return ret;
      }
    };

    int futureCount = 0;
    Future<?>[] futures = new Future<?>[par];
    for (int i = 0; i < par; i++) {
      Future<?> fut = pool.submit(() -> {
        Sorter qsort = new BatchQuick3Sort();
        for (long start = next.getAsLong(); start >= 0; start = next.getAsLong()) {
          try {
            qsort.sort(area, comparator, start, Math.min(area.size() - 1, start + width - 1));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      futures[futureCount++] = fut;
    }
    finish(futures, futureCount);
  }

  private void mergePass(ExecutorService pool,
                         SortArea<?> area,
                         Comparator<ByteBuffer> comparator,
                         SortIndex arrayA,
                         SortIndex arrayB,
                         long low,
                         long size,
                         long width) throws IOException {

    //btreeSystem.out.println("width: " + width);
    int futureCount = 0;
    Future<?>[] futures = new Future<?>[par];
    // Array A is full of runs of length width.
    final LongSupplier next = new LongSupplier() {
      long i = low;

      @Override
      public synchronized long getAsLong() {
        if (i >= size + low) {
          return -1l;
        }
        long ret = i;
        i = i + 2 * width;
        return ret;
      }
    };
    for (int i = 0; i < par; i++) {
      Future<?> fut = pool.submit(() -> {
        try {
          for (long start = next.getAsLong(); start >= 0; start = next.getAsLong()) {
            bottomUpMerge(comparator,
                          area,
                          arrayA,
                          start,
                          Math.min(start + width, size),
                          Math.min(start + 2 * width, size),
                          arrayB);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      futures[futureCount++] = fut;
    }
    finish(futures, futureCount);
  }

  private void finish(Future<?>[] futs, int futureCount) throws IOException {
    try {
      for (int i = 0; i < futureCount; i++) {
        futs[i].get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  // Left run is A[left :right-1].
  // Right run is A[right:end-1  ].
  private void bottomUpMerge(Comparator<ByteBuffer> comparator,
                             SortArea<?> area,
                             SortIndex arrayA,
                             long left,
                             long right,
                             long end,
                             SortIndex arrayB) throws IOException {
    long i = left;
    long j = right;
    // While there are elements in the left or right runs...
    for (long k = left; k < end; k++) {
      // If left run head exists and is <= existing right run head.
      if (i < right && (j >= end || comparator.compare(area.fetchK(i), area.fetchK(j)) <= 0)) {
        arrayB.set(k, arrayA.addressOf(i));
        i = i + 1;
      } else {
        arrayB.set(k, arrayA.addressOf(j));
        j = j + 1;
      }
    }
  }
}
