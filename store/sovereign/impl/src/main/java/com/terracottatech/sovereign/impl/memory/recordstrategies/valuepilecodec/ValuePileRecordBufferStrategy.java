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
import com.terracottatech.sovereign.impl.memory.RecordBufferStrategy;
import com.terracottatech.sovereign.impl.model.SovereignPersistentRecord;
import com.terracottatech.store.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author cschanck
 **/
public class ValuePileRecordBufferStrategy<K extends Comparable<K>>
  implements RecordBufferStrategy<K> {

  private final SovereignDataSetConfig<K, ?> config;
  private final boolean lazily;

  private final ThreadLocal<ValuePileBufferReader<K>> threadReader;
  private final ThreadLocal<ValuePileBufferWriter<K>> threadWriter;

  public ValuePileRecordBufferStrategy(final SovereignDataSetConfig<K, ?> config,
                                       DatasetSchemaImpl schema,
                                       boolean lazily) {
    this.config = config;
    this.lazily = lazily;

    AtomicInteger timeReferencedMaximumSerializedLength = new AtomicInteger();
    timeReferencedMaximumSerializedLength.set(this.config.getTimeReferenceGenerator().maxSerializedLength());
    Type<K> type = this.config.getType();
    this.threadReader = ThreadLocal.withInitial(bufferedReaderSupplier(type, schema, lazily));
    this.threadWriter = ThreadLocal.withInitial(bufferedWriterSupplier(timeReferencedMaximumSerializedLength, type, schema));
  }

  private static <K extends Comparable<K>>
  Supplier<ValuePileBufferReader<K>> bufferedReaderSupplier(Type<K> keyType, DatasetSchemaImpl schema, boolean lazily) {
    return () -> new ValuePileBufferReader<>(keyType, schema, lazily);
  }

  private static <K extends Comparable<K>>
  Supplier<ValuePileBufferWriter<K>> bufferedWriterSupplier(AtomicInteger timeReferencedMaximumSerializedLength,
                                                            Type<K> keyType, DatasetSchemaImpl schema) {
    return () -> new ValuePileBufferWriter<>(timeReferencedMaximumSerializedLength, keyType, schema);
  }

  @Override
  public ByteBuffer toByteBuffer(SovereignPersistentRecord<K> record) {
    return threadWriter.get().toByteBuffer(config, record);
  }

  @Override
  public SovereignPersistentRecord<K> fromByteBuffer(ByteBuffer buf) {
    try {
      return threadReader.get().fromByteBuffer(config, buf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public K readKey(ByteBuffer buf) {
    return threadReader.get().readKey(buf);
  }

  @Override
  public boolean fromConsumesByteBuffer() {
    return lazily;
  }
}