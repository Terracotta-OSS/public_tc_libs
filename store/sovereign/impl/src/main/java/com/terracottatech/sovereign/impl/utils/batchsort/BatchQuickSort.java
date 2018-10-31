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
import java.util.Random;

public class BatchQuickSort implements Sorter {
  private Random rand = new Random();

  public BatchQuickSort() {
  }

  @Override
  public void sort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    long i = low;
    long j = high;

    // Get the pivot element from some where. integer prejudice. OBOB?
    int range = (int) Math.min(Integer.MAX_VALUE, high - low);
    ByteBuffer pivot = area.fetchK(low + rand.nextInt(range));

    // Divide into two lists
    while (i <= j) {
      // If the current value from the left list is smaller than the pivot
      // element then get the next element from the left list
      while (comparator.compare(area.fetchK(i), pivot) < 0) {
        i++;
      }
      // If the current value from the right list is larger than the pivot
      // element then get the next element from the right list
      while (comparator.compare(area.fetchK(j), pivot) > 0) {
        j--;
      }

      // If we have found a value in the left list which is larger than
      // the pivot element and if we have found a value in the right list
      // which is smaller than the pivot element then we exchange the
      // values.
      // As we are done we can increase i and j
      if (i <= j) {
        area.swap(i, j);
        i++;
        j--;
      }
    }
    // Recursion
    if (low < j) {
      sort(area, comparator, low, j);
    }
    if (i < high) {
      sort(area, comparator, i, high);
    }
  }

}


