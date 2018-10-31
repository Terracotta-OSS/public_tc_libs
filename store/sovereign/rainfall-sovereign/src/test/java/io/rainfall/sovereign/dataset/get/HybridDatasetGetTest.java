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
package io.rainfall.sovereign.dataset.get;

import com.terracottatech.sovereign.SovereignBufferResource;
import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.persistence.PersistenceRoot;
import com.terracottatech.sovereign.impl.persistence.hybrid.SovereignHybridStorage;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Type;
import io.rainfall.SyntaxException;
import io.rainfall.sovereign.SovereignConfig;
import io.rainfall.sovereign.dataset.AbstractDatasetTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.sovereign.SovereignDataset.Durability.LAZY;

/**
 * @author cschanck
 **/
public class HybridDatasetGetTest extends AbstractDatasetTest {

  private SovereignDataset<Long> ds;
  private SovereignConfig<Long> config;
  private SovereignHybridStorage store;

  @Before
  public void before() throws IOException, ExecutionException, InterruptedException {
    this.config = new SovereignConfig<>();
    config.addDurability(LAZY).deleteDurability(LAZY).mutateDurability(LAZY);
    this.store = new SovereignHybridStorage(new PersistenceRoot(tmp.newFolder(), PersistenceRoot.Mode
      .CREATE_NEW), SovereignBufferResource.unlimited());
    store.startupMetadata().get();
    store.startupData().get();

    this.ds = new SovereignBuilder<>(Type.LONG, FixedTimeReference.class).offheap().limitVersionsTo(1).storage(
      store).concurrency(NUM_THREADS * 2).build();
    config.addDataset((SovereignDatasetImpl<Long>) ds);
  }

  @After
  public void efter() throws IOException {
    if (ds != null) {
      ds.getStorage().destroyDataSet(ds.getUUID());
      ds = null;
    }
    if (store != null) {
      store.shutdown();
      store = null;
    }
  }

  @Test
  public void testTransientGet() throws SyntaxException {
    runPopulatedGetOrMiss(config, "transient");
  }

}
