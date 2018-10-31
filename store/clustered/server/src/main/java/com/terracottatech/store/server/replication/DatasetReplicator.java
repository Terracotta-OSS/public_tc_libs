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
package com.terracottatech.store.server.replication;

import com.terracottatech.sovereign.SovereignDataset;
import com.terracottatech.sovereign.impl.SovereignDatasetImpl;
import com.terracottatech.store.common.messages.crud.MutationMessage;
import com.terracottatech.store.server.messages.replication.CRUDDataReplicationMessage;
import com.terracottatech.store.server.messages.replication.DataReplicationMessage;
import com.terracottatech.store.server.messages.replication.MetadataReplicationMessage;
import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.MessageCodecException;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class DatasetReplicator<K extends Comparable<K>> {

  private final SovereignDatasetImpl<K> dataset;
  private final IEntityMessenger<EntityMessage, ?> entityMessenger;
  private final ThreadLocal<ReplicationInfo> invokeContext = new ThreadLocal<>();

  public DatasetReplicator(SovereignDataset<K> dataset, IEntityMessenger<EntityMessage, ?> entityMessenger) {
    this.dataset = (SovereignDatasetImpl<K>) dataset;
    this.entityMessenger = entityMessenger;
  }

  public void setChangeConsumer() {
    dataset.setMutationConsumer(bdt -> {
      boolean interrupted = false;
      try {
        ReplicationInfo replicationInfo = invokeContext.get();
        if (replicationInfo == null) {
          // This branch is for streams
          DataReplicationMessage replicationMessage = new DataReplicationMessage(bdt.index(), bdt.getData());
          CountDownLatch latch = new CountDownLatch(1);
          entityMessenger.messageSelf(replicationMessage, messageResponse -> {
            latch.countDown();
          });
          while (true) {
            try {
              latch.await();
              break;
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } else {
          // This branch is for CRUD ops
          InvokeContext context = replicationInfo.getContext();
          MutationMessage<K> originalMessage = replicationInfo.getMessage();

          long index = bdt.index();
          ByteBuffer data = bdt.getData();
          boolean respondInFull = originalMessage.isRespondInFull();
          long clientId = context.getClientSource().toLong();
          long currentTransactionId = context.getCurrentTransactionId();
          long oldestTransactionId = context.getOldestTransactionId();

          CRUDDataReplicationMessage replicationMessage = new CRUDDataReplicationMessage(
                  index,
                  data,
                  respondInFull,
                  clientId,
                  currentTransactionId,
                  oldestTransactionId
          );

          entityMessenger.messageSelfAndDeferRetirement(originalMessage, replicationMessage);

        }
      } catch (MessageCodecException e) {
        throw new AssertionError("Message codec failure: ", e);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    });

    dataset.getStorage().addMetaDataConsumer(dataset.getUUID(), (tag, metadata) -> {
      MetadataReplicationMessage replicationMessage = new MetadataReplicationMessage(tag.ordinal(), metadata);
      try {
        entityMessenger.messageSelf(replicationMessage);
      } catch (MessageCodecException e) {
        throw new AssertionError("Message codec failure: ", e);
      }
    });

  }

  public void registerReplicationInfo(InvokeContext context, MutationMessage<K> message) {
    invokeContext.set(new ReplicationInfo(context, message));
  }

  public void clearSourceMessage() {
    invokeContext.remove();
  }

  private class ReplicationInfo {
    private final InvokeContext context;
    private final MutationMessage<K> message;

    public ReplicationInfo(InvokeContext context, MutationMessage<K> message) {
      this.context = context;
      this.message = message;
    }

    public InvokeContext getContext() {
      return context;
    }

    public MutationMessage<K> getMessage() {
      return message;
    }
  }
}
