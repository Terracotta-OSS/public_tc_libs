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
package com.terracottatech.sovereign.impl.memory.recordstrategies.valuepilecodec;

import com.terracottatech.sovereign.impl.SovereignDataSetConfig;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaImpl;
import com.terracottatech.sovereign.impl.memory.SingleRecord;
import com.terracottatech.sovereign.impl.memory.VersionedRecord;
import com.terracottatech.sovereign.testsupport.RecordGenerator;
import com.terracottatech.sovereign.time.SystemTimeReference;
import com.terracottatech.store.Cell;
import com.terracottatech.store.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by cschanck on 10/4/2016.
 */
public class ValuePileRecordBufferStrategyTest {

  static class RoundTripper<K extends Comparable<K>> {

    private final ValuePileRecordBufferStrategy<K> strat;

    public RoundTripper(SovereignDataSetConfig<K, ?> config) {
      this.strat = new ValuePileRecordBufferStrategy<K>(config, new DatasetSchemaImpl(), false);
    }

    @SuppressWarnings("unchecked")
    void roundTripRecord(VersionedRecord<K> rec) throws IOException {
      ByteBuffer buf = strat.toByteBuffer(rec);
      Object got = strat.fromByteBuffer(buf);
      assertEquals(got.getClass(), rec.getClass());
      assertThat(rec, is(got));
      assertTrue(rec.deepEquals((VersionedRecord<K>) got));
    }
  }

  private static final SystemTimeReference.Generator GENERATOR = new SystemTimeReference.Generator();

  @Test
  public void testSimple() throws IOException {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT,
                                                                                               SystemTimeReference.class)
      .timeReferenceGenerator(GENERATOR)
      .freeze();

    VersionedRecord<Integer> vr = new VersionedRecord<>();

    final SingleRecord<Integer> rec = new SingleRecord<>(vr,
                                                         10,
                                                         config.getTimeReferenceGenerator().get(),
                                                         1000L,
                                                         Cell.cell("foo", "all for one"),
                                                         Cell.cell("bar", 10),
                                                         Cell.cell("bingo", false),
                                                         Cell.cell("badminton", 100l),
                                                         Cell.cell("baz", new byte[] { 10, 11 }));
    vr.elements().add(rec);
    RoundTripper<Integer> r = new RoundTripper<>(config);
    r.roundTripRecord(vr);
  }

  @Test
  public void batchTest() throws IOException {
    SovereignDataSetConfig<Integer, SystemTimeReference> config = new SovereignDataSetConfig<>(Type.INT,
                                                                                               SystemTimeReference.class)
      .timeReferenceGenerator(GENERATOR)
      .freeze();

    RoundTripper<Integer> r = new RoundTripper<>(config);

    RecordGenerator<Integer, SystemTimeReference> gen = new RecordGenerator<>(Integer.class, GENERATOR);

    for(int i=0;i<1000;i++) {
      VersionedRecord<Integer> vr = gen.makeRecord(8);
      r.roundTripRecord(vr);
    }
  }
}