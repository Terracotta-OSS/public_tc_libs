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
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.EntityServerService;
import org.terracotta.entity.InvokeContext;
import org.terracotta.entity.PassiveServerEntity;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.persistence.IPlatformPersistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 *
 */
public class CatalogServer implements ActiveServerEntity<CatalogReq, CatalogRsp>, PassiveServerEntity<CatalogReq, CatalogRsp> {

  private static final String CATALOG_CONFIGS = "catalog_configs";
  private static final String TYPE_NAME_SEPARATOR = ":";

  private final IPlatformPersistence persistence;
  private final HashMap<String, ClientDescriptor> locks = new HashMap<>();
  private final HashMap<String, byte[]> configMap = new HashMap<>();
  private boolean leader = false;

  public CatalogServer(IPlatformPersistence persist) {
    this.persistence = persist;

    @SuppressWarnings("rawtypes")
    ServiceLoader<EntityServerService> loader = ServiceLoader.load(EntityServerService.class);
    for (EntityServerService<?, ?> entityServerService : loader) {
      PermanentEntity annotation = entityServerService.getClass().getAnnotation(PermanentEntity.class);
      if (annotation != null) {
        String type = annotation.type();
        String[] names = annotation.names();
        for (String name : names) {
          String key = type + TYPE_NAME_SEPARATOR + name;
          this.configMap.put(key, new byte[0]);
        }
      }
    }
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {

  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    locks.entrySet().removeIf(e->e.getValue().equals(clientDescriptor));
    persistConfigs();
  }

  private void persistConfigs() {
    try {
      persistence.storeDataElement(CATALOG_CONFIGS, configMap);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public CatalogRsp invokeActive(ActiveInvokeContext<CatalogRsp> context, CatalogReq message) {
    ClientDescriptor clientDescriptor = context.getClientDescriptor();
    return internalInvoke(clientDescriptor, message);
  }

  private CatalogRsp internalInvoke(ClientDescriptor clientDescriptor, CatalogReq message) {
    boolean verify = false;
    byte[] responseData = null;
    switch (message.getCmd()) {
      case LOCK:
        verify = (locks.putIfAbsent(message.getType() + TYPE_NAME_SEPARATOR + message.getName(), clientDescriptor) == null);
        break;
      case UNLOCK:
        verify = (locks.remove(message.getType() + TYPE_NAME_SEPARATOR + message.getName()) != null);
        break;
      case REFERENCE:
        responseData = configMap.get(message.getType() + TYPE_NAME_SEPARATOR + message.getName());
        verify = responseData != null;
        break;
      case RELEASE:
        responseData = configMap.get(message.getType() + TYPE_NAME_SEPARATOR + message.getName());
        verify = responseData != null;
        break;
      case CHECK_LEADER:
        verify = leader;
        break;
      case MAKE_LEADER:
        verify = !leader;
        leader = true;
        break;
      case RELEASE_LEADER:
        verify = leader;
        leader = false;
        break;
      case GET_CONFIG:
        responseData = configMap.get(message.getType() + TYPE_NAME_SEPARATOR + message.getName());
        verify = responseData != null;
        break;
      case STORE_CONFIG:
        responseData = configMap.put(message.getType() + TYPE_NAME_SEPARATOR + message.getName(), message.getPayload());
        persistConfigs();
        verify = true;
        break;
      case REMOVE_CONFIG:
        responseData = configMap.remove(message.getType() + TYPE_NAME_SEPARATOR + message.getName());
        if (responseData != null) {
          verify = true;
          persistConfigs();
        } else {
          verify = false;
        }
        break;
      case SYNC_CONFIG:
        DataInput input = new DataInputStream(new ByteArrayInputStream(message.getPayload()));
        try {
          int count = input.readInt();
          for (int x = 0; x < count; x++) {
            read(input);
          }
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
        break;
      case LIST_ALL: {
        Map<String, String> result = new HashMap<>();
        for (String key : configMap.keySet()) {
          String[] split = key.split(TYPE_NAME_SEPARATOR, 2);
          String type = split[0];
          String name = split[1];
          result.put(name, type);
        }
        responseData = CatalogCodec.encodeMapStringString(result);
        break;
      }
      case LIST_BY_TYPE: {
        Map<String, byte[]> result = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : configMap.entrySet()) {
          String[] split = entry.getKey().split(TYPE_NAME_SEPARATOR, 2);
          String type = split[0];
          if (!type.equals(message.getType())) {
            continue;
          }
          byte[] config = entry.getValue();
          String name = split[1];
          result.put(name, config);
        }
        responseData = CatalogCodec.encodeMapStringByteArray(result);
        break;
      }
    }
    return new CatalogRsp(verify, responseData == null ? null : ByteBuffer.wrap(responseData));
  }

  @Override
  public ReconnectHandler startReconnect() {
    return (clientDescriptor, extendedReconnectData) -> {
      String[] clocks = CatalogCodec.decodeReconnect(extendedReconnectData);
      for (String lock : clocks) {
        locks.put(lock, clientDescriptor);
      }
    };
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<CatalogReq> syncChannel, int concurrencyKey) {
    ByteArrayOutputStream base = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(base);
    try {
      out.writeInt(this.configMap.size());
      this.configMap.forEach((key,val)->write(out, key, val));
      out.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    syncChannel.synchronizeToPassive(new CatalogReq(CatalogCommand.SYNC_CONFIG, "", "", base.toByteArray()));
  }

  private static void write(DataOutput out, String key, byte[] val) {
    try {
      out.writeUTF(key);
      out.writeInt(val.length);
      out.write(val);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void read(DataInput in) {
    try {
      String key = in.readUTF();
      if (key != null) {
        byte[] val = new byte[in.readInt()];
        in.readFully(val);
        this.configMap.put(key, val);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void createNew() {

  }

  @Override
  public void loadExisting() {
    try {
      @SuppressWarnings("unchecked")
      Map<String, byte[]> map = (Map) persistence.loadDataElement(CATALOG_CONFIGS);
      if (map != null) {
        this.configMap.putAll(map);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void destroy() {

  }

  @Override
  public void invokePassive(InvokeContext context, CatalogReq message) {
    switch (message.getCmd()) {
      case STORE_CONFIG:
      case REMOVE_CONFIG:
      case SYNC_CONFIG:
        this.internalInvoke(null, message);
        break;
      default:
        break;
    }
  }

  @Override
  public void startSyncEntity() {

  }

  @Override
  public void endSyncEntity() {
    persistConfigs();
  }

  @Override
  public void startSyncConcurrencyKey(int concurrencyKey) {

  }

  @Override
  public void endSyncConcurrencyKey(int concurrencyKey) {

  }
}
