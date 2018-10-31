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
package io.rainfall.sovereign;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.rainfall.ObjectGenerator;
import io.rainfall.Operation;
import io.rainfall.Scenario;
import io.rainfall.SequenceGenerator;
import io.rainfall.generator.IterationSequenceGenerator;
import io.rainfall.generator.RandomSequenceGenerator;
import io.rainfall.generator.sequence.Distribution;
import io.rainfall.sovereign.results.SovereignResult;
import io.rainfall.utils.NullObjectGenerator;
import io.rainfall.utils.NullSequenceGenerator;
import io.rainfall.utils.RangeMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author cschanck
 **/
public abstract class SovereignOperation<K, T> extends Operation {

  private final SovereignResult[] enums;
  protected ObjectGenerator<K> keyGenerator = NullObjectGenerator.instance();
  protected SovereignValueGenerator<T> valueGenerator = null;
  protected SequenceGenerator sequenceGenerator = NullSequenceGenerator.instance();
  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  protected SovereignOperation<?, ?> nestedOperation = null;
  @SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  protected int nestedOperationFrequency = 1;
  protected volatile boolean isDone;

  public SovereignOperation(SovereignResult res1, SovereignResult... results) {
    super();
    HashSet<SovereignResult> al = new HashSet<>();
    al.add(res1);
    for (SovereignResult e : results) {
      al.add(e);
    }
    al.add(SovereignResult.EXCEPTION);
    this.enums = al.toArray(new SovereignResult[0]);
  }

  public boolean isDone() {
    return isDone;
  }

  public static SovereignResult[] formEnumsFrom(SovereignOperation<?, ?>... ops) {
    HashSet<SovereignResult> al = new HashSet<>();
    for (SovereignOperation<?, ?> op : ops) {
      for (SovereignResult e : op.getEnums()) {
        al.add(e);
      }
    }
    return al.toArray(new SovereignResult[0]);
  }

  public static SovereignResult[] formEnumsFrom(Scenario scen) {
    HashSet<SovereignResult> al = new HashSet<>();
    for (RangeMap<Operation> rmap : scen.getOperations()) {
      for (Operation op : rmap.getAll()) {
        if (op instanceof SovereignOperation) {
          for (SovereignResult e : ((SovereignOperation) op).getEnums()) {
            al.add(e);
          }
        }
      }
    }
    return al.toArray(new SovereignResult[0]);
  }

  public SovereignResult[] getEnums() {
    return Arrays.copyOf(enums, enums.length);
  }

  public SovereignOperation<K, T> using(ObjectGenerator<K> keyGenerator, SovereignValueGenerator<T> valueGenerator) {
    if (this.keyGenerator instanceof NullObjectGenerator) {
      this.keyGenerator = keyGenerator;
    } else {
      throw new IllegalStateException("KeyGenerator already chosen.");
    }

    if (this.valueGenerator == null) {
      this.valueGenerator = valueGenerator;
    } else {
      throw new IllegalStateException("ValueGenerator already chosen.");
    }
    return this;
  }

  public SovereignOperation<K, T> sequentially() {
    if (this.sequenceGenerator instanceof NullSequenceGenerator) {
      this.sequenceGenerator = new IterationSequenceGenerator();
    } else {
      throw new IllegalStateException("SequenceGenerator already chosen.");
    }
    return this;
  }

  public SovereignOperation<K, T> atRandom(Distribution distribution, long min, long max, long width) {
    if (sequenceGenerator instanceof NullSequenceGenerator) {
      this.sequenceGenerator = new RandomSequenceGenerator(distribution, min, max, width);
    } else {
      throw new IllegalStateException("SequenceGenerator already chosen");
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public SovereignOperation<K, T> withWeight(Double weight) {
    return (SovereignOperation<K, T>) super.withWeight(weight);
  }

  // support for compound operations
  public SovereignOperation<K, T> alsoRunWithFrequency(SovereignOperation<?, ?> subOperation, int frequency) {
    this.nestedOperation = subOperation;
    this.nestedOperationFrequency = frequency;
    return this;
  }

  public void setIsDone(boolean isDone) {
    this.isDone = isDone;
  }

  @Override
  public List<String> getDescription() {
    if (enums.length == 1) {
      return Collections.singletonList(enums[0].name() + " operation");
    }
    return Arrays.stream(enums).map((l) -> {
      return l.name() + " op";
    }).collect(Collectors.toList());
  }
}
