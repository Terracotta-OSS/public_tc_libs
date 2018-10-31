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
package com.terracottatech.ehcache.common.frs;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Test various usages of {@link RestartableGenericMap}
 *
 * @author RKAV
 */
public class RestartableGenericMapTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File folder;
  private Properties properties;

  @Before
  public void setUp() throws Exception {
    folder = temporaryFolder.newFolder();
    properties = new Properties();
  }

  @Test
  public void testCreateRestartableMap() throws Exception {
    loadFrs((rs1, rs2, rs3, rs4) -> {
      rs1.put(1L, 2.2222d);
      rs1.put(1000L, 2222.2d);
      rs2.put(1, 2.1f);
      rs2.put(11, 2.11f);
      rs3.put('1', (byte)'a');
      rs3.put('a', (byte)'1');
      rs4.put("aa", new SerializableObject<>(Long.class, String.class));
    });

    loadFrs((rs1, rs2, rs3, rs4) -> {
      assertThat(rs1.get(1L), is(2.2222d));
      assertThat(rs1.get(1000L), is(2222.2d));
      assertThat(rs2.get(1), is(2.1f));
      assertThat(rs2.get(11), is(2.11f));
      assertThat(rs3.get('1'), is((byte)'a'));
      assertThat(rs3.get('a'), is((byte)'1'));
      assertThat(rs4.get("aa").keyType.toString(), containsString("Long"));
      assertThat(rs4.get("aa").valueType.toString(), containsString("String"));
      assertNull(rs1.get(11L));
      assertNull(rs4.get("nn"));
    });
  }

  private void loadFrs(Execution test) throws Exception {
    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartStore = createRestartStore(objectManager);
    RestartableGenericMap<Long, Double> rs1 = new RestartableGenericMap<>(Long.class, Double.class,
        FrsCodecFactory.toByteBuffer("test1"), restartStore);
    RestartableGenericMap<Integer, Float> rs2 = new RestartableGenericMap<>(Integer.class, Float.class,
        FrsCodecFactory.toByteBuffer("test2"), restartStore);
    RestartableGenericMap<Character, Byte> rs3 = new RestartableGenericMap<>(Character.class, Byte.class,
        FrsCodecFactory.toByteBuffer("test3"), restartStore);
    @SuppressWarnings("unchecked")
    RestartableGenericMap<String, SerializableObject<Long, String>> rs4 =
            (RestartableGenericMap<String, SerializableObject<Long, String>>)
                    (RestartableGenericMap) new RestartableGenericMap<>(
                            String.class, SerializableObject.class,
                            FrsCodecFactory.toByteBuffer("test4"), restartStore);
    objectManager.registerObject(rs1);
    objectManager.registerObject(rs2);
    objectManager.registerObject(rs3);
    objectManager.registerObject(rs4);
    restartStore.startup().get();

    test.execute(rs1, rs2, rs3, rs4);

    restartStore.shutdown();
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createRestartStore(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) throws
      RestartStoreException, IOException {
    return RestartStoreFactory.createStore(objectManager, folder, properties);
  }

  private RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<>();
  }

  private interface Execution {
    void execute(RestartableGenericMap<Long, Double> rs1,
                 RestartableGenericMap<Integer, Float> rs2,
                 RestartableGenericMap<Character, Byte> rs3,
                 RestartableGenericMap<String, SerializableObject<Long, String>> rs4);
  }

  private static class SerializableObject<K, V> implements Serializable {
    private static final long serialVersionUID = -6810797761120706067L;
    private final Class<K> keyType;
    private final Class<V> valueType;

    SerializableObject(Class<K> keyType, Class<V> valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }
  }
}
