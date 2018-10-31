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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.catalog.msgs.CatalogCodec;
import org.terracotta.catalog.msgs.CatalogReq;
import org.terracotta.catalog.msgs.CatalogRsp;
import org.terracotta.entity.EntityClientEndpoint;
import org.terracotta.entity.EntityClientService;
import org.terracotta.entity.MessageCodec;

@SuppressWarnings("rawtypes")
public class CatalogClientService implements EntityClientService<SystemCatalog, Properties, CatalogReq, CatalogRsp, Void>  {
  private final CatalogCodec CODEC = new CatalogCodec();

  @Override
  public boolean handlesEntityType(Class<SystemCatalog> cls) {
    return SystemCatalog.class.equals(cls);
  }

  @Override
  public byte[] serializeConfiguration(Properties configuration) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    if (configuration == null) {
      configuration = new Properties();
    }
    try {
      configuration.store(bos, "");
    } catch (IOException ioe) {

    }
    return bos.toByteArray();
  }

  @Override
  public Properties deserializeConfiguration(byte[] configuration) {
    ByteArrayInputStream bis = new ByteArrayInputStream(configuration);
    Properties props = new Properties();
    try {
      props.load(bis);
    } catch (IOException ioe) {

    }
    return props;
  }

  @Override
  public SystemCatalog create(EntityClientEndpoint<CatalogReq, CatalogRsp> endpoint, Void empty) {
    return new SystemCatalogClient(endpoint);
  }

  @Override
  public MessageCodec<CatalogReq, CatalogRsp> getMessageCodec() {
    return CODEC;
  }

}
