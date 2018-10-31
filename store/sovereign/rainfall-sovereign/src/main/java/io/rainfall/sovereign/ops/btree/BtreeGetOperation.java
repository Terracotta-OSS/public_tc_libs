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
package io.rainfall.sovereign.ops.btree;

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.model.ReadTx;
import io.rainfall.AssertionEvaluator;
import io.rainfall.Configuration;
import io.rainfall.sovereign.SovereignConfig;
import io.rainfall.sovereign.SovereignOperation;
import io.rainfall.sovereign.results.SovereignResult;
import io.rainfall.statistics.StatisticsHolder;

import java.util.List;
import java.util.Map;

public class BtreeGetOperation extends SovereignOperation<Long, Long> {

  public BtreeGetOperation() {
    super(SovereignResult.GET, SovereignResult.GETMISS);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void exec(StatisticsHolder stats, Map<Class<? extends Configuration>, Configuration> configurations,
                   List<AssertionEvaluator> list) {

    SovereignConfig config = (SovereignConfig) configurations.get(SovereignConfig.class);
    final long next = this.sequenceGenerator.next();
    final ABPlusTree tree = config.getTree();
    long start = getTimeInNs();
    try {
      Long k = keyGenerator.generate(next);
      Long p = null;
      try (ReadTx tx = tree.readTx()) {
        p = tx.find(k);
        tx.commit();
      }
      long end = getTimeInNs();
      if (p == null) {
        stats.record(getClass().getSimpleName(), (end - start), SovereignResult.GETMISS);
      } else {
        stats.record(getClass().getSimpleName(), (end - start), SovereignResult.GET);
      }
    } catch (Throwable e) {
      long end = getTimeInNs();
      stats.record(getClass().getSimpleName(), (end - start), SovereignResult.EXCEPTION);
      e.printStackTrace();
      throw new AssertionError(e);
    }
  }
}
