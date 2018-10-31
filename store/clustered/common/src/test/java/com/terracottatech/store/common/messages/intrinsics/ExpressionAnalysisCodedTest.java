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

package com.terracottatech.store.common.messages.intrinsics;

import com.terracottatech.store.Record;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;
import com.terracottatech.store.function.BuildablePredicate;
import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.store.intrinsics.IntrinsicPredicate;
import com.terracottatech.store.logic.BooleanAnalyzer;
import com.terracottatech.store.logic.NormalForm;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.runnel.Struct;
import org.terracotta.runnel.StructBuilder;
import org.terracotta.runnel.decoding.StructDecoder;
import org.terracotta.runnel.encoding.StructEncoder;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.store.logic.NormalForm.Type.DISJUNCTIVE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test that the logical analysis produces the same result and
 * takes roughly the same time for an original IntrinsicPredicate and
 * its encoded and decoded version.
 */
public class ExpressionAnalysisCodedTest {

  private static final String STATS_NAMES = "Cache:GetHitLatency,Cache:GetHitLatency#100,Cache:GetHitLatency#50," +
          "Cache:GetHitLatency#95,Cache:GetHitLatency#99,Cache:GetMissLatency,Cache:GetMissLatency#100," +
          "Cache:GetMissLatency#50,Cache:GetMissLatency#95,Cache:GetMissLatency#99,Cache:PutLatency," +
          "Cache:PutLatency#100,Cache:PutLatency#50,Cache:PutLatency#95,Cache:PutLatency#99," +
          "Cache:RemoveLatency,Cache:RemoveLatency#100,Cache:RemoveLatency#50,Cache:RemoveLatency#95," +
          "Cache:RemoveLatency#99,Cache:PutCount,Cache:PutRate,Cache:RemovalCount,Cache:RemovalRate," +
          "Cache:ExpirationCount,Cache:ExpirationRate,Cache:EvictionCount,Cache:EvictionRate,Cache:HitCount," +
          "Cache:HitRate,Cache:HitRatio,Cache:MissCount,Cache:MissRate,Cache:MissRatio,Clustered:RemovalCount," +
          "Clustered:RemovalRate,Clustered:PutCount,Clustered:PutRate,Clustered:ExpirationCount," +
          "Clustered:ExpirationRate,Clustered:EvictionCount,Clustered:EvictionRate,Clustered:HitCount," +
          "Clustered:HitRate,Clustered:HitRatio,Clustered:MissCount,Clustered:MissRate,Clustered:MissRatio," +
          "Disk:RemovalCount,Disk:RemovalRate,Disk:PutCount,Disk:PutRate,Disk:ExpirationCount," +
          "Disk:ExpirationRate,Disk:EvictionCount,Disk:EvictionRate,Disk:HitCount,Disk:HitRate," +
          "Disk:HitRatio,Disk:MissCount,Disk:MissRate,Disk:MissRatio,Disk:MappingCount," +
          "Disk:AllocatedByteSize,Disk:OccupiedByteSize,OffHeap:RemovalCount,OffHeap:RemovalRate," +
          "OffHeap:PutCount,OffHeap:PutRate,OffHeap:ExpirationCount,OffHeap:ExpirationRate," +
          "OffHeap:EvictionCount,OffHeap:EvictionRate,OffHeap:HitCount,OffHeap:HitRate,OffHeap:HitRatio," +
          "OffHeap:MissCount,OffHeap:MissRate,OffHeap:MissRatio,OffHeap:MappingCount,OffHeap:AllocatedByteSize," +
          "OffHeap:OccupiedByteSize,OnHeap:RemovalCount,OnHeap:RemovalRate,OnHeap:PutCount,OnHeap:PutRate," +
          "OnHeap:ExpirationCount,OnHeap:ExpirationRate,OnHeap:EvictionCount,OnHeap:EvictionRate,OnHeap:HitCount," +
          "OnHeap:HitRate,OnHeap:HitRatio,OnHeap:MissCount,OnHeap:MissRate,OnHeap:MissRatio,OnHeap:MappingCount," +
          "OnHeap:OccupiedByteSize";

  private static final LongCellDefinition TIMESTAMP = defineLong("timestamp");
  private static final StringCellDefinition NAME = defineString("name");
  private static final StringCellDefinition TYPE = defineString("type");
  private static final StringCellDefinition CAPABILITY = defineString("capability");

  private final IntrinsicCodec intrinsicCodec = new IntrinsicCodec(TestIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS);
  private final Struct myStruct = StructBuilder.newStructBuilder().struct("intrinsic", 1, intrinsicCodec.intrinsicStruct()).build();

  @SuppressWarnings("ConstantConditions")
  private static IntrinsicPredicate<Record<?>> createPredicate() {

    BuildablePredicate<Record<?>> timestampFiler = TIMESTAMP.value()
            .isGreaterThan(0L);

    Predicate<Record<?>> statisticNameFiler = Stream.of(STATS_NAMES.split(","))
            .sorted()
            .map(name -> NAME.value().is(name))
            .reduce(BuildablePredicate::or)
            .get();
    Predicate<Record<?>> typeFilter = Arrays.stream(
            "MULTI_LINE, LATENCY, COUNTER, RATE, RATIO, GAUGE, SIZE".split(", "))
            .sorted()
            .map(type -> TYPE.value().is(type))
            .reduce(BuildablePredicate::or)
            .get();
    Predicate<Record<?>> capabilityFilter = CAPABILITY.value()
            .is("StatisticsCapability");
    Predicate<Record<?>> statisticNameFiler2 = Stream.of("Cache:HitCount", "Cache:MissCount")
            .sorted()
            .map(name -> NAME.value().is(name))
            .reduce(BuildablePredicate::or)
            .get();
    BuildablePredicate<Record<?>> repeats = Stream.generate(() -> TIMESTAMP.value().isGreaterThan(0L))
            .limit(1000)
            .reduce(BuildablePredicate::or)
            .get();
    BuildablePredicate<Record<?>> filter = timestampFiler
            .and(statisticNameFiler)
            .and(typeFilter)
            .and(capabilityFilter)
            .and(statisticNameFiler2)
            .and(repeats);
    return (IntrinsicPredicate<Record<?>>) filter;
  }

  @SuppressWarnings("unchecked")
  private IntrinsicPredicate<Record<?>> encodeAndDecodePredicate(IntrinsicPredicate<Record<?>> predicate) {
    StructEncoder<?> encoder = myStruct.encoder();
    intrinsicCodec.encodeIntrinsic(encoder.struct("intrinsic"), predicate);
    ByteBuffer encoded = encoder.encode();
    encoded.rewind();
    StructDecoder<?> decoder = myStruct.decoder(encoded);
    Intrinsic decoded = intrinsicCodec.decodeIntrinsic(decoder.struct("intrinsic"));
    assertThat(predicate, equalTo(decoded));
    assertEquals(predicate.hashCode(), decoded.hashCode());
    return (IntrinsicPredicate<Record<?>>) decoded;
  }

  @Test
  public void testAnalysisResultsBeforeAndAfterCodec() {
    IntrinsicPredicate<Record<?>> predicate = createPredicate();
    IntrinsicPredicate<Record<?>> decoded = encodeAndDecodePredicate(predicate);
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> dnf1 = analyzer
            .toNormalForm(predicate, DISJUNCTIVE);
    NormalForm<Record<?>, IntrinsicPredicate<Record<?>>> dnf2 = analyzer
            .toNormalForm(decoded, DISJUNCTIVE);
    assertEquals(dnf1, dnf2);
  }

  @Ignore("This is an ad-hoc non-regression test.")
  @Test
  public void testAnalysisTimesBeforeAndAfterCodec() {
    IntrinsicPredicate<Record<?>> predicate = createPredicate();
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    IntrinsicPredicate<Record<?>> decoded = encodeAndDecodePredicate(predicate);
    BooleanAnalyzer<Record<?>> analyzer = new BooleanAnalyzer<>();
    long time1 = 0;
    long time2 = 0;
    for (int i = 0; i < 10e3; i++) {
      long start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(predicate, DISJUNCTIVE);
      time1 += mxBean.getCurrentThreadCpuTime() - start;

      start = mxBean.getCurrentThreadCpuTime();
      analyzer.toNormalForm(decoded, DISJUNCTIVE);
      time2 += mxBean.getCurrentThreadCpuTime() - start;
    }
    assertRatioWithinFactorOfTwo(time1, time2);
  }

  private void assertRatioWithinFactorOfTwo(double time1, double time2) {
    double ratio = time1 / time2;
    assertThat(ratio, Matchers.greaterThan(0.5));
    assertThat(ratio, Matchers.lessThan(2.0));
  }
}
