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
package io.rainfall.sovereign.dataset;

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
import io.rainfall.sovereign.ops.dataset.Add;
import io.rainfall.sovereign.ops.dataset.Delete;
import io.rainfall.sovereign.ops.dataset.Get;
import io.rainfall.sovereign.ops.dataset.Put;
import io.rainfall.sovereign.results.SovereignResult;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.util.LinkedList;
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
public abstract class AbstractDatasetTest {

  static int DEFAULT_NUM_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);
  static int DEFAULT_MINUTES_TO_RUN = 10;
  static int DEFAULT_MINUTES_TO_WARMUP = 0;
  static Distribution DEFAULT_DISTRIBUTION = Distribution.FLAT;
  static long DEFAULT_DISTRIBUTION_MIN = 0l;
  static long DEFAULT_DISTRIBUTION_MAX = 10000000l;
  static long DEFAULT_DISTRIBUTION_WIDTH = 1000000l;

  protected int NUM_THREADS = getIntProperty("sovereign.rainfall.numthreads", DEFAULT_NUM_THREADS);
  protected int MINUTES_TO_RUN = getIntProperty("sovereign.rainfall.runtime", DEFAULT_MINUTES_TO_RUN);
  protected int MINUTES_TO_WARMUP = getIntProperty("sovereign.rainfall.warmup", DEFAULT_MINUTES_TO_WARMUP);
  protected Distribution DISTRIBUTION = getDistribution("sovereign.rainfall.distribution", DEFAULT_DISTRIBUTION);
  protected long DISTRIBUTION_MIN = getLongProperty("sovereign.rainfall.distmin", DEFAULT_DISTRIBUTION_MIN);
  protected long DISTRIBUTION_MAX = getLongProperty("sovereign.rainfall.distmax", DEFAULT_DISTRIBUTION_MAX);
  protected long DISTRIBUTION_WIDTH = getLongProperty("sovereign.rainfall.distwidth", DEFAULT_DISTRIBUTION_WIDTH);

  private static Distribution getDistribution(String name, Distribution distribution) {
    String ret = System.getProperties().getProperty(name);
    if (ret == null) {
      return distribution;
    }
    return Distribution.valueOf(ret);
  }

  private static int getIntProperty(String name, int def) {
    String ret = System.getProperties().getProperty(name);
    if (ret == null) {
      return def;
    }
    return Integer.parseInt(ret);
  }

  private static long getLongProperty(String name, long def) {
    String ret = System.getProperties().getProperty(name);
    if (ret == null) {
      return def;
    }
    return Long.parseLong(ret);
  }

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  public ReportingConfig<?> makeReport(Scenario scenario, Class<?> clz, String tag) {
    return ReportingConfig.report(SovereignResult.class, SovereignOperation.formEnumsFrom(scenario)).log(text(),
      html("target/rainfall/" + tag + "-" + clz.getCanonicalName())).every(15, TimeUnit.SECONDS);
  }

  public ReportingConfig<?> makeTextOnlyReport(Scenario scenario) {
    return ReportingConfig.report(SovereignResult.class, SovereignOperation.formEnumsFrom(scenario)).log(text()).every(
      15, TimeUnit.SECONDS);
  }

  public void runAddOrMiss(SovereignConfig<?> config, String tag) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(NUM_THREADS).timeout(1, MINUTES);

    LongGenerator keyGenerator = new LongGenerator();
    SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

    LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();

    ops.add(new Add<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DEFAULT_DISTRIBUTION_WIDTH).withWeight(1.0));

    Scenario scenario = new Scenario("CRUD").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

    ReportingConfig<?> reporting = makeReport(scenario, this.getClass(), "addOrMiss");

    Runner.setUp(scenario).warmup(during(MINUTES_TO_WARMUP, minutes)).executed(times(100 * 1000),
      nothingFor(1, seconds), during(MINUTES_TO_RUN, minutes)).config(config, concurrency, reporting).start();
  }

  public void runAddOrReplace(SovereignConfig<?> config, String tag) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(NUM_THREADS).timeout(1, MINUTES);

    LongGenerator keyGenerator = new LongGenerator();
    SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

    LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();

    ops.add(new Put<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(1.0));

    Scenario scenario = new Scenario("CRUD").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

    ReportingConfig<?> reporting = makeReport(scenario, this.getClass(), "addOrReplace");

    Runner.setUp(scenario).warmup(during(MINUTES_TO_WARMUP, minutes)).executed(times(100 * 1000),
      nothingFor(1, seconds), during(MINUTES_TO_RUN, minutes)).config(config, concurrency, reporting).start();
  }

  public void runAddReplaceGet(SovereignConfig<?> config, String tag, double addReplaceWeight,
                               double getWeight) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(NUM_THREADS).timeout(1, MINUTES);

    LongGenerator keyGenerator = new LongGenerator();
    SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

    LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();

    ops.add(new Put<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(addReplaceWeight));
    ops.add(new Get<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(getWeight));

    Scenario scenario = new Scenario("CRUD").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

    ReportingConfig<?> reporting = makeReport(scenario, this.getClass(), "addReplaceGet");

    Runner.setUp(scenario).warmup(during(MINUTES_TO_WARMUP, minutes)).executed(times(100 * 1000),
      nothingFor(1, seconds), during(MINUTES_TO_RUN, minutes)).config(config, concurrency, reporting).start();

  }

  public void runAddReplaceGetDelete(SovereignConfig<?> config, String tag, double addReplaceWeight, double getWeight,
                                     double deleteWeight) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(NUM_THREADS).timeout(1, MINUTES);

    LongGenerator keyGenerator = new LongGenerator();
    SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

    LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();

    ops.add(new Put<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(addReplaceWeight));
    ops.add(new Get<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(getWeight));
    ops.add(new Delete<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
      DISTRIBUTION_MAX, DISTRIBUTION_WIDTH).withWeight(deleteWeight));

    Scenario scenario = new Scenario("CRUD").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

    ReportingConfig<?> reporting = makeReport(scenario, this.getClass(), "AddReplaceGetDelete");

    Runner.setUp(scenario).warmup(during(MINUTES_TO_WARMUP, minutes)).executed(times(100 * 1000),
      nothingFor(1, seconds), during(MINUTES_TO_RUN, minutes)).config(config, concurrency, reporting).start();

  }

  public void runPopulatedGetOrMiss(SovereignConfig<?> config, String tag) throws SyntaxException {
    ConcurrencyConfig concurrency = ConcurrencyConfig.concurrencyConfig().threads(NUM_THREADS).timeout(4, MINUTES);

    {
      LongGenerator keyGenerator = new LongGenerator();
      SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

      LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();
      ops.add(new Put<Long, Long>().using(keyGenerator, valueGenerator).sequentially());

      Scenario scenario = new Scenario("PopulatedGetOrMiss").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

      ReportingConfig<?> reporting = makeTextOnlyReport(scenario);

      Runner.setUp(scenario).executed(times(DISTRIBUTION_MAX - DISTRIBUTION_MIN)).
        config(config, concurrency, reporting).start();
    }
    {
      LongGenerator keyGenerator = new LongGenerator();
      SovereignLongGenerator valueGenerator = new SovereignLongGenerator();

      LinkedList<SovereignOperation<?, ?>> ops = new LinkedList<>();
      ops.add(new Get<Long, Long>().using(keyGenerator, valueGenerator).atRandom(DISTRIBUTION, DISTRIBUTION_MIN,
        DISTRIBUTION_MAX, DISTRIBUTION_MAX - DISTRIBUTION_MIN).withWeight(1.0));

      Scenario scenario = new Scenario("PopulatedGetOrMiss").exec(ops.toArray(new SovereignOperation<?, ?>[] { }));

      ReportingConfig<?> reporting = makeReport(scenario, this.getClass(), "get");

      Runner.setUp(scenario).warmup(during(10, seconds)).executed(during(MINUTES_TO_RUN, minutes)).config(config,
        concurrency, reporting).start();
    }
  }

}
