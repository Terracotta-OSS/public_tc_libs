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
package com.terracottatech.ehcache.clustered.server;

import org.junit.Test;

import static com.terracottatech.ehcache.clustered.server.ResourcePoolManager.getMaxSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ResourcePoolManagerTest {
  @Test
  public void testGetMaxSize() throws Exception {
    assertThat(getMaxSize(128 * 1024), is(4 * 1024L));
    assertThat(getMaxSize(256 * 1024), is(8 * 1024L));
    assertThat(getMaxSize(512 * 1024), is(16 * 1024L));
    assertThat(getMaxSize(1024 * 1024), is(32 * 1024L));
    assertThat(getMaxSize(2 * 1024 * 1024), is(64 * 1024L));
    assertThat(getMaxSize(4 * 1024 * 1024), is(128 * 1024L));
    assertThat(getMaxSize(64 * 1024 * 1024), is(2 * 1024 * 1024L));
    assertThat(getMaxSize(256 * 1024 * 1024), is(8 * 1024 * 1024L));
    assertThat(getMaxSize(512 * 1024 * 1024), is(8 * 1024 * 1024L));
    assertThat(getMaxSize(1024 * 1024 * 1024), is(8 * 1024 * 1024L));
    assertThat(getMaxSize(1024 * 1024 * 1024 * 1024L), is(8 * 1024 * 1024L));
  }

}