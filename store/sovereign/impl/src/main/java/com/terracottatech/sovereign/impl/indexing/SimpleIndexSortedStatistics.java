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
import com.terracottatech.sovereign.impl.model.SovereignSortedIndexMap;
import com.terracottatech.sovereign.indexing.SovereignSortedIndexStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SimpleIndexSortedStatistics extends SimpleIndexStatistics implements SovereignSortedIndexStatistics {
  private static final Logger LOGGER= LoggerFactory.getLogger(SimpleIndexSortedStatistics.class);

  static final SovereignSortedIndexStatistics NOOP = new SovereignSortedIndexStatistics() {
    @Override
    public long indexFirstCount() {
      return 0;
    }

    @Override
    public long indexLastCount() {
      return 0;
    }

    @Override
    public long indexGetCount() {
      return 0;
    }

    @Override
    public long indexHigherCount() {
      return 0;
    }

    @Override
    public long indexHigherEqualCount() {
      return 0;
    }

    @Override
    public long indexLowerCount() {
      return 0;
    }

    @Override
    public long indexLowerEqualCount() {
      return 0;
    }

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
      return this;
    }

    @Override
    public String toString() {
      return "Sorted Index Empty Stats";
    }
  };

  public SimpleIndexSortedStatistics(SimpleIndex<?, ?> index) {
    super(index);
  }

  @Override
  public long indexFirstCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupFirstCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexLastCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupLastCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexGetCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupEqualCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexHigherCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupHigherCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexHigherEqualCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupHigherEqualCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexLowerCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupLowerCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public long indexLowerEqualCount() {
    try {
      return ((SovereignSortedIndexMap) index.getUnderlyingIndex()).statLookupLowerEqualCount();
    } catch (Exception e) {
      LOGGER.debug("Oddity fetching statistic", e);
      return 0;
    }
  }

  @Override
  public SovereignSortedIndexStatistics getSortedStatistics() {
    return this;
  }

  public String toString() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    pw.println("Index statistics:");
    pw.println("  occupied size = " + MiscUtils.bytesAsNiceString(occupiedStorageSize()));
    pw.println("  count = " + indexedRecordCount());
    pw.println("  access count = " + indexAccessCount());
    pw.println("  first accesses = " + indexFirstCount());
    pw.println("  last accesses = " + indexLastCount());
    pw.println("  equal accesses = " + indexGetCount());
    pw.println("  higher accesses = " + indexHigherCount());
    pw.println("  higher equal accesses = " + indexHigherEqualCount());
    pw.println("  lower accesses = " + indexLowerCount());
    pw.print("  lower equal accesses = " + indexLowerEqualCount());
    pw.flush();
    return sw.toString();

  }
}
