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

public class BatchQuick3Sort implements Sorter {

  @Override
  public void sort(SortArea<?> area, Comparator<ByteBuffer> comparator, long low, long high) throws IOException {
    if (high <= low) {
      return;
    }
    long lt = low;
    long gt = high;
    ByteBuffer v = area.fetchK(low);
    long i = low;
    while (i <= gt) {
      int cmp = comparator.compare(area.fetchK(i),v);
      if (cmp < 0) {
        area.swap(lt++, i++);
      } else if (cmp > 0) {
        area.swap(i, gt--);
      } else {
        i++;
      }
    }

    sort(area, comparator, low, lt - 1);
    sort(area, comparator, gt + 1, high);
  }
}
