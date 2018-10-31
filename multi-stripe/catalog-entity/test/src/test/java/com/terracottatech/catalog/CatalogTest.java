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
package com.terracottatech.catalog;

import com.terracottatech.testing.AbstractEnterprisePassthroughTest;
import com.terracottatech.testing.calculator.Calculator;
import com.terracottatech.testing.calculator.CalculatorCodec;
import com.terracottatech.testing.calculator.CalculatorConfig;
import com.terracottatech.testing.calculator.CalculatorEntityClientService;
import com.terracottatech.testing.calculator.CalculatorEntityServerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.lease.connection.LeasedConnectionFactory;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 * @author Ludovic Orban
 */
public class CatalogTest extends AbstractEnterprisePassthroughTest {

  private Connection connection;
  private SystemCatalog systemCatalog;

  @Before
  public void setUp() throws Exception {
    String connectionName = "Test:Connection:" + UUID.randomUUID();

    Properties properties = new Properties();
    properties.put(ConnectionPropertyNames.CONNECTION_NAME, connectionName);
    properties.put(ConnectionPropertyNames.CONNECTION_TIMEOUT, "10000");

    connection = LeasedConnectionFactory.connect(buildClusterUri(), properties);
    systemCatalog = fetchSystemCatalog(connection);
  }

  @After
  public void tearDown() throws Exception {
    connection.close();
  }

  @Override
  protected List<String> provideStripeNames() {
    return Collections.unmodifiableList(Arrays.asList("stripe1", "stripe2"));
  }

  @Override
  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    return server -> {
      server.registerClientEntityService(new CalculatorEntityClientService());
      server.registerServerEntityService(new CalculatorEntityServerService());
    };
  }

  @Test
  public void testSystemCatalogContainsSytemCatalog() throws Exception {
    byte[] configuration = systemCatalog.getConfiguration(SystemCatalog.class, SystemCatalog.ENTITY_NAME);
    assertThat(configuration, notNullValue());
  }

  @Test
  public void testListAll() throws Exception {
    int size = systemCatalog.listAll().size();
    assertThat(systemCatalog.listAll().get(SystemCatalog.ENTITY_NAME), is(SystemCatalog.class.getName()));

    systemCatalog.storeConfiguration(Calculator.class, "calculator1", CalculatorCodec.serializeConfiguration(new CalculatorConfig(null)));
    assertThat(systemCatalog.listAll().get(SystemCatalog.ENTITY_NAME), is(SystemCatalog.class.getName()));
    assertThat(systemCatalog.listAll().get("calculator1"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().size(), is(size + 1));

    systemCatalog.storeConfiguration(Calculator.class, "calculator2", CalculatorCodec.serializeConfiguration(new CalculatorConfig(null)));
    assertThat(systemCatalog.listAll().get(SystemCatalog.ENTITY_NAME), is(SystemCatalog.class.getName()));
    assertThat(systemCatalog.listAll().get("calculator1"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().get("calculator2"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().size(), is(size + 2));

    systemCatalog.storeConfiguration(Calculator.class, "calculator3", CalculatorCodec.serializeConfiguration(new CalculatorConfig(null)));
    assertThat(systemCatalog.listAll().get(SystemCatalog.ENTITY_NAME), is(SystemCatalog.class.getName()));
    assertThat(systemCatalog.listAll().get("calculator1"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().get("calculator2"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().get("calculator3"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().size(), is(size + 3));

    systemCatalog.removeConfiguration(Calculator.class, "calculator1");
    assertThat(systemCatalog.listAll().get(SystemCatalog.ENTITY_NAME), is(SystemCatalog.class.getName()));
    assertThat(systemCatalog.listAll().get("calculator2"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().get("calculator3"), is("com.terracottatech.testing.calculator.Calculator"));
    assertThat(systemCatalog.listAll().size(), is(size + 2));
  }

  @Test
  public void testListByType() throws Exception {
    assertThat(systemCatalog.listByType(Calculator.class), is(Collections.<String, byte[]>emptyMap()));
    systemCatalog.storeConfiguration(Calculator.class, "calculator1", CalculatorCodec.serializeConfiguration(new CalculatorConfig(null)));
    Map<String, byte[]> map1 = systemCatalog.listByType(Calculator.class);
    assertThat(map1.keySet(), is(Collections.singleton("calculator1")));
    assertThat(CalculatorCodec.deserializeConfiguration(map1.get("calculator1")), is(new CalculatorConfig(null)));

    systemCatalog.storeConfiguration(Calculator.class, "calculator2", CalculatorCodec.serializeConfiguration(new CalculatorConfig("casio")));
    Map<String, byte[]> map2 = systemCatalog.listByType(Calculator.class);
    assertThat(map2.keySet(), is(asSet("calculator2", "calculator1")));
    assertThat(CalculatorCodec.deserializeConfiguration(map2.get("calculator1")), is(new CalculatorConfig(null)));
    assertThat(CalculatorCodec.deserializeConfiguration(map2.get("calculator2")), is(new CalculatorConfig("casio")));
  }

  @Test
  public void testGetConfiguration() throws Exception {
    connection.getEntityRef(Calculator.class, 1, "calculator1").create(null);
    assertThat(CalculatorCodec.deserializeConfiguration(systemCatalog.getConfiguration(Calculator.class, "calculator1")), is(nullValue()));

    connection.getEntityRef(Calculator.class, 1, "calculator2").create(new CalculatorConfig("HP"));
    assertThat(CalculatorCodec.deserializeConfiguration(systemCatalog.getConfiguration(Calculator.class, "calculator2")), is(new CalculatorConfig("HP")));
  }

  @Test
  public void testRemoveConfiguration() throws Exception {
    connection.getEntityRef(Calculator.class, 1, "calculator1").create(null);
    assertThat(systemCatalog.getConfiguration(Calculator.class, "calculator1"), is(notNullValue()));
    systemCatalog.removeConfiguration(Calculator.class, "calculator1");
    assertThat(systemCatalog.getConfiguration(Calculator.class, "calculator1"), is(nullValue()));
  }

  @Test
  public void testLocking() throws Exception {
    assertThat(systemCatalog.tryLock(Calculator.class, "calculator1"), is(true));
    assertThat(systemCatalog.tryLock(Calculator.class, "calculator1"), is(false));
    assertThat(systemCatalog.unlock(Calculator.class, "calculator1"), is(true));
    assertThat(systemCatalog.unlock(Calculator.class, "calculator1"), is(false));
    assertThat(systemCatalog.tryLock(Calculator.class, "calculator1"), is(true));
    assertThat(systemCatalog.unlock(Calculator.class, "calculator1"), is(true));
  }

  private static Set<String> asSet(String... strings) {
    return new HashSet<String>(Arrays.asList(strings));
  }

}
