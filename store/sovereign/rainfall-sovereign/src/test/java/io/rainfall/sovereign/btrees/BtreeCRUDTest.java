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
package io.rainfall.sovereign.btrees;

import com.terracottatech.sovereign.btrees.bplustree.appendonly.ABPlusTree;
import com.terracottatech.sovereign.btrees.bplustree.appendonly.DUT;
import io.rainfall.Runner;
import io.rainfall.Scenario;
import io.rainfall.SyntaxException;
import io.rainfall.configuration.ConcurrencyConfig;
import io.rainfall.configuration.ReportingConfig;
import io.rainfall.generator.LongGenerator;
import io.rainfall.generator.sequence.Distribution;
import io.rainfall.sovereign.SovereignConfig;
import io.rainfall.sovereign.SovereignLongGenerator;
import io.rainfall.sovereign.SovereignOperation;
import io.rainfall.sovereign.ops.btree.BtreeDeleteOperation;
import io.rainfall.sovereign.ops.btree.BtreePutOperation;
import io.rainfall.sovereign.results.SovereignResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.rainfall.configuration.ReportingConfig.html;
import static io.rainfall.configuration.ReportingConfig.text;
import static io.rainfall.execution.Executions.during;
import static io.rainfall.execution.Executions.nothingFor;
import static io.rainfall.execution.Executions.times;
import static io.rainfall.unit.TimeDivision.minutes;
import static io.rainfall.unit.TimeDivision.seconds;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author cschanck
 **/
public class BtreeCRUDTest {

  private ABPlusTree<?> tree;

  @Before
  public void before() throws IOException {
    this.tree = DUT.offheap(64);
  }

  @After
  public void efter() throws IOException {
    if (tree != null) {
      tree.close();
    }
    tree = null;
  }

  @Test
  @Ignore
  public void testReadWriteDelete() throws SyntaxException {
    SovereignConfig<?> config = new SovereignConfig<>();
    config.setTree(tree);

    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(8).timeout(1, MINUTES);
    LongGenerator keyGenerator = new LongGenerator();
    SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

    Scenario scenario = new Scenario("Data Access Phase").exec(
      new BtreePutOperation().using(keyGenerator, valueGenerator).atRandom(Distribution.FLAT, 0, 100000,
        10000).withWeight(0.5),
      new BtreeDeleteOperation().using(keyGenerator, valueGenerator).atRandom(Distribution.FLAT, 0, 100000,
        10000).withWeight(0.5) );

    ReportingConfig<?> reporting = ReportingConfig.report(SovereignResult.class,
      SovereignOperation.formEnumsFrom(scenario)).log(text(), html("target/BtreeCRUD")).every(15, TimeUnit.SECONDS);

    Runner.setUp(scenario).warmup(during(0, seconds)).executed(times(100 * 1000), nothingFor(1, seconds),
      during(2, minutes)).config(config, concurrency, reporting).start();
  }
}
