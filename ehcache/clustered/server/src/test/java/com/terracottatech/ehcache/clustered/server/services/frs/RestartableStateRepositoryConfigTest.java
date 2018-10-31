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

package com.terracottatech.ehcache.clustered.server.services.frs;

import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author vmad
 */
public class RestartableStateRepositoryConfigTest {
  @Test
  public void testRunnelSupport() throws Exception {
    final RestartableStateRepositoryConfig restartableStateRepositoryConfig =
      new RestartableStateRepositoryConfig("CACHE_ID", new FrsDataLogIdentifier("a", "b", "c"));
    restartableStateRepositoryConfig.setObjectIndex(0);

    final ByteBuffer encodeBytes = restartableStateRepositoryConfig.encode();
    final RestartableStateRepositoryConfig decodedConfig = new RestartableStateRepositoryConfig(encodeBytes);

    assertThat(decodedConfig.getObjectIndex(), is(restartableStateRepositoryConfig.getObjectIndex()));
    assertThat(decodedConfig.getParentId(), is(restartableStateRepositoryConfig.getParentId()));
    assertThat(decodedConfig.getDataLogIdentifier(), is(restartableStateRepositoryConfig.getDataLogIdentifier()));
  }
}