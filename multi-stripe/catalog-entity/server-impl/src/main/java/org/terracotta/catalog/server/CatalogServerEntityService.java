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
package org.terracotta.catalog.server;

import com.tc.classloader.PermanentEntity;
import org.terracotta.catalog.msgs.CatalogCodec;
import org.terracotta.catalog.msgs.CatalogCommand;
import org.terracotta.catalog.msgs.CatalogReq;
import org.terracotta.catalog.msgs.CatalogRsp;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ConcurrencyStrategy;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.ExecutionStrategy;
import org.terracotta.entity.MessageCodec;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.ServiceException;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.entity.SyncMessageCodec;
import org.terracotta.persistence.IPlatformPersistence;

import java.util.Collections;
import java.util.Set;

/**
 *  annotation must match the type, and version below for this to work
 */
@PermanentEntity(type="org.terracotta.catalog.SystemCatalog", names={"staticCataloger"}, version=1)
public class CatalogServerEntityService implements EntityServerService<CatalogReq, CatalogRsp> {

  private static final ConcurrencyStrategy<CatalogReq> CONCURRENCY = new ConcurrencyStrategy<CatalogReq>() {
    @Override
    public int concurrencyKey(CatalogReq message) {
      return ConcurrencyStrategy.MANAGEMENT_KEY;
    }

    @Override
    public Set<Integer> getKeysForSynchronization() {
      return Collections.singleton(1);
    }

  };

  private static final ExecutionStrategy<CatalogReq> EXECUTION = m -> ExecutionStrategy.Location.BOTH;

  @Override
  public long getVersion() {
    return 1L;
  }

  @Override
  public boolean handlesEntityType(String typeName) {
    return "org.terracotta.catalog.SystemCatalog".equals(typeName);
  }

  @Override
  public ActiveServerEntity<CatalogReq, CatalogRsp> createActiveEntity(ServiceRegistry registry, byte[] configuration) {
    try {
      return new CatalogServer(registry.getService(new BasicServiceConfiguration<>(IPlatformPersistence.class)));
    } catch (ServiceException e) {
      throw new AssertionError("Failed to obtain the singleton IPlatformPersistence instance", e);
    }
  }

  @Override
  public PassiveServerEntity<CatalogReq, CatalogRsp> createPassiveEntity(ServiceRegistry registry, byte[] configuration) {
    try {
      return new CatalogServer(registry.getService(new BasicServiceConfiguration<>(IPlatformPersistence.class)));
    } catch (ServiceException e) {
      throw new AssertionError("Failed to obtain the singleton IPlatformPersistence instance", e);
    }
  }

  @Override
  public ConcurrencyStrategy<CatalogReq> getConcurrencyStrategy(byte[] configuration) {
    return  CONCURRENCY;
  }

  @Override
  public ExecutionStrategy<CatalogReq> getExecutionStrategy(byte[] configuration) {
    return EXECUTION;
  }

  @Override
  public MessageCodec<CatalogReq, CatalogRsp> getMessageCodec() {
    return new CatalogCodec();
  }

  @Override
  public SyncMessageCodec<CatalogReq> getSyncMessageCodec() {
    return new SyncMessageCodec<CatalogReq>() {
      @Override
      public byte[] encode(int i, CatalogReq m) throws MessageCodecException {
        return m.getPayload();
      }

      @Override
      public CatalogReq decode(int i, byte[] bytes) throws MessageCodecException {
        return new CatalogReq(CatalogCommand.SYNC_CONFIG, "", "", bytes);
      }
    };
  }

}
