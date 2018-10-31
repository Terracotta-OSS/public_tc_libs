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
package com.terracottatech.tools.server;

import org.junit.Before;
import org.junit.Test;
import org.terracotta.entity.ConfigurationException;
import org.terracotta.entity.IEntityMessenger;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.monitoring.PlatformService;

import com.terracottatech.tools.validation.Validator;
import com.terracottatech.tools.config.ClusterConfiguration;
import com.terracottatech.tools.config.Server;
import com.terracottatech.tools.config.Stripe;

import java.util.ArrayList;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TopologyEntityServerServiceTest {

  private static final ClusterConfiguration CLUSTER_CONFIGURATION = new ClusterConfiguration("test", new ArrayList<>());
  private static final ClusterConfiguration STARTUP_CONFIGURATION;
  private ServiceRegistry registry = mock(ServiceRegistry.class);
  private Validator validator = mock(Validator.class);

  static {
    Stripe stripe = new Stripe(singletonList(new Server("test", "test")));
    STARTUP_CONFIGURATION = new ClusterConfiguration("test", singletonList(stripe));
  }

  @Before
  public void setUp() throws Exception {
    when(registry.getService(any(ServiceConfiguration.class))).thenAnswer(invocation -> {
      Class<?> serviceType = ((ServiceConfiguration) invocation.getArgument(0)).getServiceType();
      if (serviceType.equals(PlatformService.class)) {
        return mock(PlatformService.class);
      } else if (serviceType.equals(IEntityMessenger.class)) {
        return mock(IEntityMessenger.class);
      }
      return null;
    });
  }

  @Test
  public void testActiveEntityCreation_NullEntityConfigDefaultsToStartupConfig() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return null;
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return CLUSTER_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        return validator;
      }
    };

    TopologyActiveEntity topologyActiveEntity = service.createActiveEntity(registry, new byte[1]);
    assertThat(topologyActiveEntity.getClusterConfiguration(), is(CLUSTER_CONFIGURATION));
  }

  @Test
  public void testActiveEntityCreation_NonNullClusterConfig_Validation_PASS() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return CLUSTER_CONFIGURATION;
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return STARTUP_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        return validator;
      }
    };

    TopologyActiveEntity topologyActiveEntity = service.createActiveEntity(registry, new byte[1]);
    assertThat(topologyActiveEntity.getClusterConfiguration(), is(CLUSTER_CONFIGURATION));
  }

  @Test(expected = ConfigurationException.class)
  public void testEntityCreation_NonNullClusterConfig_Validation_FAIL() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return mock(ClusterConfiguration.class);
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return STARTUP_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        doThrow(IllegalArgumentException.class).when(validator).validateStripeAgainstCluster(any(), any());
        return validator;
      }
    };

    service.createActiveEntity(registry, new byte[1]);
  }

  @Test
  public void testPassiveEntityCreation_NullEntityConfigDefaultsToStartupConfig() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return null;
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return CLUSTER_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        return validator;
      }
    };

    TopologyPassiveEntity topologyPassiveEntity = service.createPassiveEntity(registry, new byte[1]);
    assertThat(topologyPassiveEntity.getClusterConfiguration(), is(CLUSTER_CONFIGURATION));
  }

  @Test
  public void testPassiveEntityCreation_NonNullClusterConfig_Validation_PASS() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return CLUSTER_CONFIGURATION;
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return STARTUP_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        return validator;
      }
    };

    TopologyPassiveEntity topologyPassiveEntity = service.createPassiveEntity(registry, new byte[1]);
    assertThat(topologyPassiveEntity.getClusterConfiguration(), is(CLUSTER_CONFIGURATION));
  }

  @Test(expected = ConfigurationException.class)
  public void testPassiveCreation_NonNullClusterConfig_Validation_FAIL() throws Exception {
    TopologyEntityServerService service = new TopologyEntityServerService() {
      @Override
      ClusterConfiguration getEntityConfiguration(byte[] configuration) {
        return mock(ClusterConfiguration.class);
      }

      @Override
      ClusterConfiguration getStartupConfiguration(PlatformService platformService) {
        return STARTUP_CONFIGURATION;
      }

      @Override
      Validator getValidator() {
        doThrow(IllegalArgumentException.class).when(validator).validateStripeAgainstCluster(any(), any());
        return validator;
      }
    };

    service.createPassiveEntity(registry, new byte[1]);
  }

}