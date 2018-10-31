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
package com.terracottatech.store.server.messages;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignBuilder;
import com.terracottatech.sovereign.impl.SovereignDatasetDescriptionImpl;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.sovereign.impl.dataset.metadata.DatasetSchemaBackend;
import com.terracottatech.sovereign.impl.dataset.metadata.PersistableSchemaList;
import com.terracottatech.sovereign.impl.dataset.metadata.SchemaCellDefinition;
import com.terracottatech.sovereign.impl.persistence.base.MetadataKey;
import com.terracottatech.sovereign.impl.utils.CachingSequence;
import com.terracottatech.sovereign.indexing.SovereignIndexSettings;
import com.terracottatech.sovereign.time.FixedTimeReference;
import com.terracottatech.store.Type;
import com.terracottatech.store.common.messages.DatasetEntityMessage;
import com.terracottatech.store.common.messages.intrinsics.IntrinsicCodec;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.server.ServerIntrinsicDescriptors;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import com.terracottatech.store.server.messages.replication.SyncBoundaryMessage;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.entity.MessageCodecException;

import java.nio.ByteBuffer;

import static com.terracottatech.store.definition.CellDefinition.define;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DatasetServerMessageCodecTest {

  private final DatasetServerMessageCodec codec = new DatasetServerMessageCodec(new IntrinsicCodec(ServerIntrinsicDescriptors.OVERRIDDEN_DESCRIPTORS));

  @Test
  public void testDataReplicationMessage() throws Exception {
    byte[] buff = "hello".getBytes();
    DataReplicationMessage replicationMessage = new DataReplicationMessage(5L, ByteBuffer.wrap(buff));
    DataReplicationMessage decoded = (DataReplicationMessage) codec.decodeMessage(codec.encodeMessage(replicationMessage));
    assertThat(decoded.getIndex(), is(replicationMessage.getIndex()));
    ByteBuffer decodedData = decoded.getData();
    for (byte b : buff) {
      assertThat(decodedData.get(), is(b));
    }
  }

  @Test
  public void testDataReplicationMessageNullBuffer() throws Exception {
    DataReplicationMessage replicationMessage = new DataReplicationMessage(5L, null);
    DataReplicationMessage decoded = (DataReplicationMessage) codec.decodeMessage(codec.encodeMessage(replicationMessage));
    assertThat(decoded.getIndex(), is(replicationMessage.getIndex()));
    assertThat(decoded.getData(), nullValue());
  }

  @Test
  public void testEncodeDecodeCachingSequenceSync() throws MessageCodecException {
    CachingSequence cachingSequence = new CachingSequence(10, 1000);
    MetadataReplicationMessage metadataReplicationMessage =
            new MetadataReplicationMessage(MetadataKey.Tag.CACHING_SEQUENCE.ordinal(), cachingSequence);

    byte[] bytes = this.codec.encodeMessage(metadataReplicationMessage);

    DatasetEntityMessage datasetEntityMessage = this.codec.decodeMessage(bytes);

    MetadataReplicationMessage decoded = (MetadataReplicationMessage) datasetEntityMessage;

    Assert.assertThat(decoded.getIndex(), equalTo(metadataReplicationMessage.getIndex()));

    CachingSequence decodedCachingSequence = (CachingSequence)decoded.getMetadata();
    Assert.assertThat(decodedCachingSequence.getCacheMany(), equalTo(cachingSequence.getCacheMany()));
    Assert.assertThat(decodedCachingSequence.getNextChunk(), equalTo(cachingSequence.getNextChunk()));
    Assert.assertThat(decodedCachingSequence.current(), equalTo(cachingSequence.current()));
  }

  @Test
  public void testEncodeDecodeDatasetSchemaBackendSync() throws MessageCodecException {
    DatasetSchemaBackend backend = new DatasetSchemaBackend();
    for (int i = 0; i < 100; i++) {
      int p = i % 5;
      CellDefinition<String> def = define("foo" + p, Type.STRING);
      SchemaCellDefinition<?> got = backend.idFor(def);
      Assert.assertThat(got, CoreMatchers.notNullValue());
      Assert.assertThat(got.definition(), Is.is(def));
    }

    MetadataReplicationMessage metadataReplicationMessage =
        new MetadataReplicationMessage(MetadataKey.Tag.SCHEMA.ordinal(), backend.getPersistable());

    byte[] bytes = this.codec.encodeMessage(metadataReplicationMessage);

    DatasetEntityMessage datasetEntityMessage = this.codec.decodeMessage(bytes);

    MetadataReplicationMessage decoded = (MetadataReplicationMessage) datasetEntityMessage;

    Assert.assertThat(decoded.getIndex(), equalTo(metadataReplicationMessage.getIndex()));

    PersistableSchemaList persistableSchemaList = (PersistableSchemaList)decoded.getMetadata();
    Assert.assertThat(persistableSchemaList.getDefinitions().size(), Is.is(5));
  }

  @Test
  public void testEncodeDecodeSoverignDatasetDecriptionSync() throws Exception {
    SovereignDataset<Integer> ds =
            new SovereignBuilder<>(Type.INT, FixedTimeReference.class).heap().build();
    ds.getIndexing().createIndex(CellDefinition.define("foo", Type.STRING), SovereignIndexSettings.btree()).call();
    SovereignDatasetImpl<Integer> impl = (SovereignDatasetImpl<Integer>) ds;

    SovereignDatasetDescriptionImpl<Integer, ?> descr = impl.getDescription();

    MetadataReplicationMessage metadataReplicationMessage = new MetadataReplicationMessage(MetadataKey.Tag
                                                                                              .DATASET_DESCR.ordinal(), descr);

    byte[] bytes = this.codec.encodeMessage(metadataReplicationMessage);

    DatasetEntityMessage datasetEntityMessage = this.codec.decodeMessage(bytes);

    MetadataReplicationMessage decoded = (MetadataReplicationMessage) datasetEntityMessage;

    Assert.assertThat(decoded.getIndex(), equalTo(metadataReplicationMessage.getIndex()));

    @SuppressWarnings("unchecked")
    SovereignDatasetDescriptionImpl<Integer, ?> decodedDescr = (SovereignDatasetDescriptionImpl)decoded.getMetadata();

    Assert.assertThat(decodedDescr.getConfig().getConcurrency(), Matchers.is(impl.getConfig().getConcurrency()));
    Assert.assertThat(decodedDescr.getConfig().getResourceSize(), Matchers.is(impl.getConfig().getResourceSize()));
    Assert.assertThat(decodedDescr.getConfig().getStorageType(), Matchers.is(impl.getConfig().getStorageType()));
    Assert.assertThat(decodedDescr.getConfig().getVersionLimitStrategy().type().getName(), Matchers.is(impl.getConfig().getVersionLimitStrategy().type().getName()));
    Assert.assertThat(decodedDescr.getConfig().getTimeReferenceGenerator(), equalTo(null));
    Assert.assertThat(decodedDescr.getConfig().getRecordLockTimeout(), Matchers.is(impl.getConfig().getRecordLockTimeout()));
    Assert.assertThat(decodedDescr.getConfig().getType(), Matchers.is(impl.getConfig().getType()));
    Assert.assertThat(decodedDescr.getConfig().getVersionLimit(), Matchers.is(impl.getConfig().getVersionLimit()));
    Assert.assertThat(decodedDescr.getUUID(), Matchers.is(impl.getUUID()));
    Assert.assertThat(decodedDescr.getAlias(), Matchers.is(impl.getUUID().toString()));
    Assert.assertThat(decodedDescr.getIndexDescriptions().size(), Matchers.is(1));
  }

  @Test
  public void testCRUDDataReplicationMessage() throws Exception {
    byte[] buff = "hello".getBytes();
    CRUDDataReplicationMessage replicationMessage = new CRUDDataReplicationMessage(5L, ByteBuffer.wrap(buff), true, 5L, 123L, 567L);
    CRUDDataReplicationMessage decoded = (CRUDDataReplicationMessage) codec.decodeMessage(codec.encodeMessage(replicationMessage));
    assertThat(decoded.getIndex(), is(replicationMessage.getIndex()));
    ByteBuffer decodedData = decoded.getData();
    for (byte b : buff) {
      assertThat(decodedData.get(), is(b));
    }
    assertThat(decoded.isRespondInFull(), is(replicationMessage.isRespondInFull()));
    assertThat(decoded.getClientId(), is(replicationMessage.getClientId()));
    assertThat(decoded.getCurrentTransactionId(), is(replicationMessage.getCurrentTransactionId()));
    assertThat(decoded.getOldestTransactionId(), is(replicationMessage.getOldestTransactionId()));
  }

  @Test
  public void testSyncBoundaryMessage() throws Exception {
    SyncBoundaryMessage syncBoundaryMessage = new SyncBoundaryMessage(2);
    SyncBoundaryMessage decoded = (SyncBoundaryMessage) codec.decodeMessage(codec.encodeMessage(syncBoundaryMessage));

    assertThat(decoded.getShardIndex(), is(2));
  }
}
