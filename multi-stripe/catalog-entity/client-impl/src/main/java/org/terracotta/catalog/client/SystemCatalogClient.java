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
package org.terracotta.catalog.client;

import org.terracotta.catalog.SystemCatalog;
import org.terracotta.catalog.msgs.CatalogCodec;
import org.terracotta.catalog.msgs.CatalogCommand;
import org.terracotta.catalog.msgs.CatalogReq;
import org.terracotta.catalog.msgs.CatalogRsp;
import org.terracotta.entity.EndpointDelegate;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.exception.EntityException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SystemCatalogClient implements SystemCatalog {

  private final EntityClientEndpoint<CatalogReq, CatalogRsp> endpoint;
  private final Set<String> locked = new HashSet<>();

  public SystemCatalogClient(EntityClientEndpoint<CatalogReq, CatalogRsp> endpoint) {
    this.endpoint = endpoint;
    this.endpoint.setDelegate(new EndpointDelegate<CatalogRsp>() {
      @Override
      public void handleMessage(CatalogRsp messageFromServer) {

      }

      @Override
      public byte[] createExtendedReconnectData() {
        String[] array = locked.toArray(new String[locked.size()]);
        return CatalogCodec.encodeReconnect(array);
      }

      @Override
      public void didDisconnectUnexpectedly() {

      }
    });
  }

  @Override
  public boolean makeLeader() {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.MAKE_LEADER, null, null, null)).invoke().get().result();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public boolean releaseLeader() {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.RELEASE_LEADER, null, null, null)).invoke().get().result();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public boolean isLeader() {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.CHECK_LEADER, null, null, null)).invoke().get().result();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public byte[] getConfiguration(Class<?> type, String name) {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.GET_CONFIG, type.getName(), name, null)).invoke().get().getPayload();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public byte[] storeConfiguration(Class<?> type, String name, byte[] config) {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.STORE_CONFIG, type.getName(), name, config)).invoke().get().getPayload();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public byte[] removeConfiguration(Class<?> type, String name) {
    try {
      return endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.REMOVE_CONFIG, type.getName(), name, null)).invoke().get().getPayload();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public Map<String, String> listAll() {
    try {
      byte[] payload = endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.LIST_ALL, null, null, null)).invoke().get().getPayload();
      return CatalogCodec.decodeMapStringString(payload);
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public Map<String, byte[]> listByType(Class<?> type) {
    try {
      byte[] payload = endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.LIST_BY_TYPE, type.getName(), null, null)).invoke().get().getPayload();
      return CatalogCodec.decodeMapStringByteArray(payload);
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void referenceConfiguration(Class<?> type, String name) {
    try {
      endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.REFERENCE, type.getName(), name, null)).invoke().get();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void releaseConfiguration(Class<?> type, String name) {
    try {
      endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.RELEASE, type.getName(), name, null)).invoke().get();
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public synchronized boolean tryLock(Class<?> type, String name) {
    try {
      boolean result = endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.LOCK, type.getName(), name, null)).invoke().get().result();
      if (result) {
        locked.add(type.getName() + ":" + name);
      }
      return result;
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public synchronized boolean unlock(Class<?> type, String name) {
    try {
      boolean result = endpoint.beginInvoke().message(new CatalogReq(CatalogCommand.UNLOCK, type.getName(), name, null)).invoke().get().result();
      if (result) {
        locked.remove(type.getName() + ":" + name);
      }
      return result;
    } catch (MessageCodecException code) {
      throw new RuntimeException(code);
    } catch (EntityException ee) {
      throw new RuntimeException(ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void close() {
    endpoint.close();
  }
}
