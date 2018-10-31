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

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class BatchMergeSortTest extends AbstractSortTest {
  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Override
  public SortArea<ByteBuffer> makeArea() throws IOException {
    //return SortArea.areaOf(new SortIndex.Naive(), new SortScratchPad.Naive());
    //return SortArea.areaOf(new OffheapSortIndex(4096), new FileChannelScratchPad(tmp.newFile()));
    return SortArea.areaOf(new OffheapSortIndex(4096), new MappedScratchPad(tmp.newFile()));
  }

  @Override
  public void sort(SortArea<ByteBuffer> a, Comparator<ByteBuffer> comp) throws IOException {
    new BatchParallelMergeSort().sort(a, comp);
  }

}