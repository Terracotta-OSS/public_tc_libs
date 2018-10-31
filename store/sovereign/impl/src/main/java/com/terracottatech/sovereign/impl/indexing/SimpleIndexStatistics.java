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
package com.terracottatech.sovereign.impl.indexing;

import com.terracottatech.sovereign.common.utils.MiscUtils;
import com.terracottatech.sovereign.indexing.SovereignIndexStatistics;
import com.terracottatech.sovereign.indexing.SovereignSortedIndexStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SimpleIndexStatistics implements SovereignIndexStatistics {
  private static final Logger LOGGER= LoggerFactory.getLogger(SimpleIndexStatistics.class);

  static final SovereignIndexStatistics NOOP = new SovereignIndexStatistics() {
    @Override
    public long indexedRecordCount() {
      return 0;
    }

    @Override
    public long occupiedStorageSize() {
      return 0;
    }

    @Override
    public long indexAccessCount() {
      return 0;
    }

    @Override
    public SovereignSortedIndexStatistics getSortedStatistics() {
      return null;
    }

    @Override
    public String toString() {
      return "Empty Index Statistics";
    }
  };

  final SimpleIndex<?, ?> index;
  private final boolean sorted;

  public SimpleIndexStatistics(SimpleIndex<?, ?> index) {
    this.sorted = index.getDescription().getIndexSettings().isSorted();
    this.index = index;
  }

  @Override
  public long indexedRecordCount() {
    try {
      return index.getUnderlyingIndex().estimateSize();
    } catch (Throwable e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexAccessCount() {
    try {
      return index.getUnderlyingIndex().statAccessCount();
    } catch (Throwable e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public SovereignSortedIndexStatistics getSortedStatistics() {
    return null;
  }

  @Override
  public long occupiedStorageSize() {
    try {
      return index.getUnderlyingIndex().getOccupiedStorageSize();
    } catch (Throwable e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Index statistics:");
    pw.println("  occupied size = " + MiscUtils.bytesAsNiceString(occupiedStorageSize()));
    pw.println("  count = " + indexedRecordCount());
    pw.print("  access count = " + indexAccessCount());
    pw.flush();
    return sw.toString();
  }
}
