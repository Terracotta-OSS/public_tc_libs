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
package com.terracottatech.store.server.sync;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaBackend;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import com.terracottatech.sovereign.impl.memory.BufferDataTuple;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.DatasetEntityResponse;
import com.terracottatech.store.common.messages.crud.AddRecordSimplifiedResponse;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.server.ServerIntrinsicDescriptors;
import com.terracottatech.store.server.messages.SyncDatasetCodec;
import com.terracottatech.store.server.messages.replication.BatchedDataSyncMessage;
import com.terracottatech.store.server.messages.replication.MessageTrackerSyncMessage;
import com.terracottatech.store.server.messages.replication.MetadataSyncMessage;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.entity.MessageCodecException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.terracottatech.store.definition.CellDefinition.define;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SyncDatasetCodecTest {

  private final SyncDatasetCodec syncDatasetCodec = new SyncDatasetCodec(new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));

  @Test
  public void testEncodeDecodeCachingSequenceSync() throws MessageCodecException {
    CachingSequence cachingSequence = new CachingSequence(10, 1000);
    MetadataSyncMessage metadataSyncMessage = new MetadataSyncMessage(cachingSequence, MetadataKey.Tag.CACHING_SEQUENCE.ordinal());

    byte[] bytes = this.syncDatasetCodec.encode(1, metadataSyncMessage);

    DatasetEntityMessage datasetEntityMessage = this.syncDatasetCodec.decode(1, bytes);

    MetadataSyncMessage decoded = (MetadataSyncMessage) datasetEntityMessage;

    assertThat(decoded.getTagKey(), equalTo(metadataSyncMessage.getTagKey()));

    CachingSequence decodedCachingSequence = (CachingSequence)decoded.getMetadata();
    assertThat(decodedCachingSequence.getCacheMany(), equalTo(cachingSequence.getCacheMany()));
    assertThat(decodedCachingSequence.getNextChunk(), equalTo(cachingSequence.getNextChunk()));
    assertThat(decodedCachingSequence.current(), equalTo(cachingSequence.current()));
  }

  @Test
  public void testEncodeDecodeDatasetSchemaBackendSync() throws MessageCodecException {
    DatasetSchemaBackend backend = new DatasetSchemaBackend();
    for (int i = 0; i < 100; i++) {
      int p = i % 5;
      CellDefinition<String> def = define("foo" + p, Type.STRING);
      SchemaCellDefinition<?> got = backend.idFor(def);
      assertThat(got, notNullValue());
      assertThat(got.definition(), Is.is(def));
    }

    MetadataSyncMessage metadataSyncMessage = new MetadataSyncMessage(backend.getPersistable(), MetadataKey.Tag.SCHEMA.ordinal());

    byte[] bytes = this.syncDatasetCodec.encode(1, metadataSyncMessage);

    DatasetEntityMessage datasetEntityMessage = this.syncDatasetCodec.decode(1, bytes);

    MetadataSyncMessage decoded = (MetadataSyncMessage) datasetEntityMessage;

    assertThat(decoded.getTagKey(), equalTo(metadataSyncMessage.getTagKey()));

    PersistableSchemaList persistableSchemaList = (PersistableSchemaList)decoded.getMetadata();
    assertThat(persistableSchemaList.getDefinitions().size(), Is.is(5));
  }

  @Test
  public void testEncodeDecodeSoverignDatasetDecriptionSync() throws Exception {
    SovereignDataset<Integer> ds =
        new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();
    SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;

    SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();

    MetadataSyncMessage metadataSyncMessage = new MetadataSyncMessage(descr, MetadataKey.Tag.DATASET_DESCR.ordinal());

    byte[] bytes = this.syncDatasetCodec.encode(1, metadataSyncMessage);

    DatasetEntityMessage datasetEntityMessage = this.syncDatasetCodec.decode(1, bytes);

    MetadataSyncMessage decoded = (MetadataSyncMessage) datasetEntityMessage;

    assertThat(decoded.getTagKey(), equalTo(metadataSyncMessage.getTagKey()));

    @SuppressWarnings("unchecked")
    SovereignDatasetDescriptionImpl<Integer, ?> decodedDescr = (SovereignDatasetDescriptionImpl) decoded.getMetadata();

    Assert.assertThat(decodedDescr.getConfig().getConcurrency(), is(impl.getConfig().getConcurrency()));
    Assert.assertThat(decodedDescr.getConfig().getResourceSize(), is(impl.getConfig().getResourceSize()));
    Assert.assertThat(decodedDescr.getConfig().getStorageType(), is(impl.getConfig().getStorageType()));
    Assert.assertThat(decodedDescr.getConfig().getVersionLimitStrategy().type().getName(), is(impl.getConfig().getVersionLimitStrategy().type().getName()));
    Assert.assertThat(decodedDescr.getConfig().getTimeReferenceGenerator(), equalTo(null));
    Assert.assertThat(decodedDescr.getConfig().getRecordLockTimeout(), is(impl.getConfig().getRecordLockTimeout()));
    Assert.assertThat(decodedDescr.getConfig().getType(), is(impl.getConfig().getType()));
    Assert.assertThat(decodedDescr.getConfig().getVersionLimit(), is(impl.getConfig().getVersionLimit()));
    Assert.assertThat(decodedDescr.getUUID(), is(impl.getUUID()));
    Assert.assertThat(decodedDescr.getAlias(), is(impl.getUUID().toString()));
    Assert.assertThat(decodedDescr.getIndexDescriptions().size(), is(1));
  }

  @Test
  public void testEncodeDecodeBatchedSyncMessage() throws MessageCodecException {
    List<BufferDataTuple> bufferDataTuples = new ArrayList<>();

    for (int i = 1; i <= 10; i++) {
      int finalI = i;
      bufferDataTuples.add(new BufferDataTuple() {
        @Override
        public long index() {
          return finalI;
        }

        @Override
        public ByteBuffer getData() {
          return getBuffer();
        }
      });
    }

    BatchedDataSyncMessage batchedDataSyncMessage = new BatchedDataSyncMessage(bufferDataTuples, 2);

    byte[] encoded = this.syncDatasetCodec.encode(1, batchedDataSyncMessage);

    DatasetEntityMessage datasetEntityMessage = this.syncDatasetCodec.decode(1, encoded);

    BatchedDataSyncMessage decoded = (BatchedDataSyncMessage) datasetEntityMessage;

    assertThat(decoded.getShardIdx(), equalTo(batchedDataSyncMessage.getShardIdx()));
    assertThat(decoded.getDataTuples().size(), equalTo(batchedDataSyncMessage.getDataTuples().size()));
    decoded.getDataTuples().forEach(x -> {
      ByteBuffer bytes = x.getData();

      for (byte i = 0; i < 50; i++) {
        assertThat(bytes.get(), is(i));
      }
    });
  }

  @Test
  public void testEncodeDecodeMessageTrackerSyncMessage() throws MessageCodecException {
    long clientId = 5;
    int segmentIndex = 7;
    Map<Long, DatasetEntityResponse> responses = new HashMap<>();
    responses.put(1L, new AddRecordSimplifiedResponse(true));
    responses.put(2L, new AddRecordSimplifiedResponse(false));
    MessageTrackerSyncMessage syncMessage = new MessageTrackerSyncMessage(clientId, responses, segmentIndex);

    byte[] bytes = this.syncDatasetCodec.encode(1, syncMessage);

    MessageTrackerSyncMessage decodedMessage = (MessageTrackerSyncMessage) this.syncDatasetCodec.decode(1, bytes);
    assertThat(decodedMessage.getClientId(), is(clientId));
    assertThat(decodedMessage.getSegmentIndex(), is(segmentIndex));
    Map<Long, DatasetEntityResponse> decodedResponses = decodedMessage.getTrackedResponses();
    for (Map.Entry<Long, DatasetEntityResponse> entry : responses.entrySet()) {
      AddRecordSimplifiedResponse decodedResponse = (AddRecordSimplifiedResponse) decodedResponses.get(entry.getKey());
      AddRecordSimplifiedResponse originalResponse = (AddRecordSimplifiedResponse) responses.get(entry.getKey());
      assertThat(decodedResponse.isAdded(), is(originalResponse.isAdded()));
    }
  }

  private static ByteBuffer getBuffer() {
    ByteBuffer bytes = ByteBuffer.allocate(50);
    for (byte i = 0; i < 50; i++) {
      bytes.put(i);
    }
    bytes.flip();
    return bytes;
  }
}