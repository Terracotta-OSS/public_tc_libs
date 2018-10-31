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

package com.terracottatech.sovereign.impl.memory;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.store.Type;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by cschanck on 10/3/2016.
 */
public class SovereignRuntimeTest {

  @Test
  public void testManyShardsSingleFinalizer() throws Exception {
    Set<Thread> start = getSovereignFinalizerThreads();
    final SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT,
                                                                                                     SystemTimeReference.class);
    config.concurrency(64);
    SovereignRuntime<?> run = new SovereignRuntime<>(config, new CachingSequence());
    assertThat(run.getShardEngine().getShardCount(), is(64));
    Set<Thread> end = getSovereignFinalizerThreads();
    end.removeAll(start);
    assertThat(end.size(), is(1));
    run.dispose();
  }

  private Set<Thread> getSovereignFinalizerThreads() {
    Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
    Set<Thread> ret = traces.keySet().stream()
      .filter(thread -> thread.toString().toLowerCase().contains("sovereign finalizer"))
      .collect(Collectors.toSet());
    return ret;
  }
}