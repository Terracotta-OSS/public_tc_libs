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
package com.terracottatech.ehcache.disk.internal.frsbacked.frsmgr;

import org.ehcache.core.spi.store.Store.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.terracottatech.ehcache.common.frs.metadata.FrsDataLogIdentifier;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link SavedStoreConfig}
 *
 * @author RKAV
 */
public class SavedStoreConfigTest {
  private Configuration<Long, String> mockedConfiguration;
  private Configuration<Long, Long> mockedConfiguration1;
  private Configuration<String, Long> mockedConfiguration2;
  private final FrsDataLogIdentifier dataLogId = new FrsDataLogIdentifier("root", "default", "default");

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    mockedConfiguration = mock(Configuration.class);
    when(mockedConfiguration.getKeyType()).thenReturn(Long.class);
    when(mockedConfiguration.getValueType()).thenReturn(String.class);
    mockedConfiguration1 = mock(Configuration.class);
    when(mockedConfiguration1.getKeyType()).thenReturn(Long.class);
    when(mockedConfiguration1.getValueType()).thenReturn(Long.class);
    mockedConfiguration2 = mock(Configuration.class);
    when(mockedConfiguration2.getKeyType()).thenReturn(String.class);
    when(mockedConfiguration2.getValueType()).thenReturn(Long.class);
  }

  @Test
  public void testSavedStoreConfig() throws Exception {
    SavedStoreConfig configUnderTest = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    assertThat(configUnderTest.getSizeInBytes(), is(2048*1024L));
  }

  @Test
  public void testEncodeAndDecode() throws Exception {
    SavedStoreConfig configUnderTest = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    ByteBuffer bb = configUnderTest.encode();
    configUnderTest = new SavedStoreConfig(bb);
    assertThat(configUnderTest.getSizeInBytes(), is(2048*1024L));
    assertThat(configUnderTest.toString(), containsString("java.lang.Long"));
    assertThat(configUnderTest.toString(), containsString("java.lang.String"));
  }

  @Test
  public void testValidate() throws Exception {
    SavedStoreConfig configUnderTest = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    SavedStoreConfig otherConfig = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    configUnderTest.validate(otherConfig);
  }

  @Test
  public void testValidateWithWrongConfig() throws Exception {
    SavedStoreConfig configUnderTest = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    SavedStoreConfig otherConfig1 = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1023);
    SavedStoreConfig otherConfig2 = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    try {
      configUnderTest.validate(otherConfig1);
      fail("Validation succeeded despite differences in store size");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Cannot change FRS resource pool size"));
    }
  }

  @Test
  public void testValidateWithWrongTypes() throws Exception {
    SavedStoreConfig configUnderTest = new SavedStoreConfig(dataLogId, "default", mockedConfiguration, 2048*1024);
    SavedStoreConfig otherConfig1 = new SavedStoreConfig(dataLogId, "default", mockedConfiguration1, 2048*1024);
    SavedStoreConfig otherConfig2 = new SavedStoreConfig(dataLogId, "default", mockedConfiguration2, 2048*1024);
    try {
      configUnderTest.validate(otherConfig1);
      fail("Validation succeeded despite differences in value types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Value type"));
    }
    try {
      configUnderTest.validate(otherConfig2);
      fail("Validation succeeded despite differences in key types");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Key type"));
    }
  }
}
