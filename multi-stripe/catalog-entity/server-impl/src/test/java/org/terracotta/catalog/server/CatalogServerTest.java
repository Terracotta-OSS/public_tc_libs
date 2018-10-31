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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.terracotta.catalog.msgs.CatalogCodec;
import org.terracotta.catalog.msgs.CatalogReq;
import org.terracotta.catalog.msgs.CatalogRsp;
import org.terracotta.entity.ActiveInvokeContext;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.persistence.IPlatformPersistence;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.terracotta.catalog.msgs.CatalogCommand.LIST_ALL;
import static org.terracotta.catalog.msgs.CatalogCommand.LIST_BY_TYPE;
import static org.terracotta.catalog.msgs.CatalogCommand.STORE_CONFIG;

/**
 * CatalogServerTest
 */
@SuppressWarnings("unchecked")
public class CatalogServerTest {

  @Test
  public void testSyncMessageResultsInPersistence() throws Exception {
    IPlatformPersistence platformPersistence = mock(IPlatformPersistence.class);
    CatalogServer catalogServer = new CatalogServer(platformPersistence);

    CatalogReq syncMessage = createSyncMessage(platformPersistence, catalogServer);

    catalogServer.invokePassive(null, syncMessage);
    catalogServer.endSyncEntity();
    verify(platformPersistence).storeDataElement(any(String.class), any(HashMap.class));
  }

  @Test
  public void testEmptyStringDatasetNames() throws Exception {
    IPlatformPersistence platformPersistence = mock(IPlatformPersistence.class);
    CatalogServer catalogServer = new CatalogServer(platformPersistence);
    CatalogReq catalogReq = new CatalogReq(STORE_CONFIG, "test", "", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq);

    CatalogReq listAllReq = new CatalogReq(LIST_ALL, null, null, new byte[0]);
    CatalogRsp resp1 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listAllReq);
    Assert.assertThat(CatalogCodec.decodeMapStringString(resp1.getPayload()).size(), is(2));

    CatalogReq listTypeReq = new CatalogReq(LIST_BY_TYPE, "test", null, new byte[0]);
    CatalogRsp resp2 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listTypeReq);
    Assert.assertThat(CatalogCodec.decodeMapStringByteArray(resp2.getPayload()).size(), is(1));
  }

  @Test
  public void testNonEmptyStringDatasetNames() throws Exception {
    IPlatformPersistence platformPersistence = mock(IPlatformPersistence.class);
    CatalogServer catalogServer = new CatalogServer(platformPersistence);
    CatalogReq catalogReq = new CatalogReq(STORE_CONFIG, "test", "foo", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq);

    CatalogReq listAllReq = new CatalogReq(LIST_ALL, null, null, new byte[0]);
    CatalogRsp resp1 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listAllReq);
    Assert.assertThat(CatalogCodec.decodeMapStringString(resp1.getPayload()).size(), is(2));

    CatalogReq listTypeReq = new CatalogReq(LIST_BY_TYPE, "test", null, new byte[0]);
    CatalogRsp resp2 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listTypeReq);
    Assert.assertThat(CatalogCodec.decodeMapStringByteArray(resp2.getPayload()).size(), is(1));

  }

  @Test
  public void testMixedStringDatasetNames() throws Exception {
    IPlatformPersistence platformPersistence = mock(IPlatformPersistence.class);
    CatalogServer catalogServer = new CatalogServer(platformPersistence);
    CatalogReq catalogReq1 = new CatalogReq(STORE_CONFIG, "test", "", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq1);
    CatalogReq catalogReq2 = new CatalogReq(STORE_CONFIG, "test", "f:oo", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq2);
    CatalogReq catalogReq3 = new CatalogReq(STORE_CONFIG, "test", ":foo", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq3);
    CatalogReq catalogReq4 = new CatalogReq(STORE_CONFIG, "test", "foo", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq4);

    CatalogReq listAllReq = new CatalogReq(LIST_ALL, null, null, new byte[0]);
    CatalogRsp resp1 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listAllReq);
    Assert.assertThat(CatalogCodec.decodeMapStringString(resp1.getPayload()).size(), is(5));

    CatalogReq listTypeReq = new CatalogReq(LIST_BY_TYPE, "test", null, new byte[0]);
    CatalogRsp resp2 = catalogServer.invokeActive(mock(ActiveInvokeContext.class), listTypeReq);
    Map<String, byte[]> map = CatalogCodec.decodeMapStringByteArray(resp2.getPayload());
    Assert.assertThat(map.size(), is(4));
    Assert.assertThat(map.containsKey(""), is(true));
    Assert.assertThat(map.containsKey("f:oo"), is(true));
    Assert.assertThat(map.containsKey(":foo"), is(true));
    Assert.assertThat(map.containsKey("foo"), is(true));
  }

  private CatalogReq createSyncMessage(IPlatformPersistence platformPersistence, CatalogServer catalogServer) {
    CatalogReq catalogReq = new CatalogReq(STORE_CONFIG, "test", "test", new byte[0]);
    catalogServer.invokeActive(mock(ActiveInvokeContext.class), catalogReq);
    PassiveSynchronizationChannel<CatalogReq> syncChannel = mock(PassiveSynchronizationChannel.class);
    ArgumentCaptor<CatalogReq> captor = ArgumentCaptor.forClass(CatalogReq.class);
    doNothing().when(syncChannel).synchronizeToPassive(captor.capture());
    catalogServer.synchronizeKeyToPassive(syncChannel, 1);

    reset(platformPersistence);
    return captor.getValue();
  }

}