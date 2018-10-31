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

public class BatchMergeSort implements Sorter {
  @Override
  public void sort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    mergeSort(area, comparator, low, high);
  }

  private void mergeSort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    // Each 1-element run in A is already "sorted".
    // Make successively longer sorted runs of length 2, 4, 8, 16... until whole array is sorted.
    SortIndex arrayA = area.getIndex();
    SortIndex arrayB = arrayA.duplicate();
    long size = high - low + 1;

    for (long width = 1; width < size; width = 2 * width) {
      mergePass(area, comparator, arrayA, arrayB, low, size, width);

      area.setIndex(arrayB);
      arrayB=arrayA;
      arrayA=area.getIndex();
    }
  }

  private void mergePass(SortArea<?> area,
                         Comparator<ByteBuffer> comparator,
                         SortIndex arrayA,
                         SortIndex arrayB, long low, long size,
                         long width) throws IOException {

    //System.out.println("width: "+width);

    // Array A is full of runs of length width.
    for (long i = low; i < size+low; i = i + 2 * width) {
      // Merge two runs: A[i:i+width-1] and A[i+width:i+2*width-1] to B[]
      // or copy A[i:n-1] to B[] ( if(i+width >= n) )
      bottomUpMerge(comparator, area, arrayA, i, Math.min(i + width, size), Math.min(i + 2 * width, size), arrayB);
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
