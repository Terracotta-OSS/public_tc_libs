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

package com.terracottatech.ehcache.clustered.server.state;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.ehcache.common.frs.FrsCodecFactory;
import com.terracottatech.ehcache.common.frs.RestartableGenericMap;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RestartableInvalidationTrackerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testIsClearInProgress() throws Exception {
    RestartableGenericMap<Long, Integer> restartableMap = getRestartableMap("foo");
    restartableMap.put(Long.MIN_VALUE, 1);
    RestartableInvalidationTracker tracker = new RestartableInvalidationTracker(restartableMap);
    assertThat(tracker.isClearInProgress(), is(true));
    restartableMap.remove(Long.MIN_VALUE);
    assertThat(tracker.isClearInProgress(), is(false));
  }

  @Test
  public void setClearInProgress() throws Exception {
    RestartableGenericMap<Long, Integer> restartableMap = getRestartableMap("foo");
    RestartableInvalidationTracker tracker = new RestartableInvalidationTracker(restartableMap);
    tracker.setClearInProgress(true);
    assertThat(restartableMap.containsKey(Long.MIN_VALUE), is(true));
    tracker.setClearInProgress(false);
    assertThat(restartableMap.containsKey(Long.MIN_VALUE), is(false));
  }

  RestartableGenericMap<Long, Integer> getRestartableMap(String id) throws IOException, RestartStoreException, InterruptedException, ExecutionException {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = new RegisterableObjectManager<>();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore =
        RestartStoreFactory.createStore(objectManager, temporaryFolder.newFolder(), new Properties());
    RestartableGenericMap<Long, Integer> restartableMap =
        new RestartableGenericMap<>(Long.class, Integer.class, FrsCodecFactory.toByteBuffer(id), restartStore);
    objectManager.registerObject(restartableMap);
    restartStore.startup().get();
    return restartableMap;
  }

}