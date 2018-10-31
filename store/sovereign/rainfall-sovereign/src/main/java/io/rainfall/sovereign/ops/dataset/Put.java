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
package io.rainfall.sovereign.ops.dataset;

import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Record;
import com.terracottatech.store.Type;
import com.terracottatech.store.definition.CellDefinition;
import io.rainfall.AssertionEvaluator;
import io.rainfall.Configuration;
import io.rainfall.Execution;
import io.rainfall.sovereign.SovereignConfig;
import io.rainfall.sovereign.SovereignOperation;
import io.rainfall.sovereign.results.SovereignResult;
import io.rainfall.statistics.StatisticsHolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Put<K extends Comparable<K>, T> extends SovereignOperation<K, T> {

  private String cName = getClass().getSimpleName();

  public Put() {
    super(SovereignResult.ADD, SovereignResult.REPLACE);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void exec(final StatisticsHolder statisticsHolder,
                   final Map<Class<? extends Configuration>, Configuration> configurations,
                   final List<AssertionEvaluator> assertions) {

    @SuppressWarnings("unchecked")
    SovereignConfig<K> sovConfig = (SovereignConfig<K>) configurations.get(SovereignConfig.class);
    CellDefinition<Long> def = CellDefinition.define("counter", Type.LONG);
    final long next = this.sequenceGenerator.next();

    for (final SovereignDatasetImpl<K> ds : sovConfig) {
      if (!ds.isDisposed()) {
        Cell<Long> l = def.newCell((Long) valueGenerator.generate(next));
        K k = keyGenerator.generate(next);
        long start = getTimeInNs();
        try {
          Record<K> prior = ds.add(sovConfig.getAddDurability(), k, l);
          long end = getTimeInNs();
          if (prior == null) {
            statisticsHolder.record(cName, (end - start), SovereignResult.ADD);
          } else {
            ds.applyMutation(sovConfig.getMutateDurability(), k, r -> true, r -> {
              Optional<Long> opt = r.get(def);
              Cell<Long> c = def.newCell(opt.get() + 1);
              return Arrays.asList(c);
            });
            statisticsHolder.record(cName, (end - start), SovereignResult.REPLACE);
          }
        } catch (Throwable e) {
          if (getExecutionState() != Execution.ExecutionState.ENDING) {
            long end = getTimeInNs();
            statisticsHolder.record(cName, (end - start), SovereignResult.EXCEPTION);
            e.printStackTrace();
          }
        }
      }
    }
  }

}
