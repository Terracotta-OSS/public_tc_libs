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
package com.terracottatech.testing;

import com.terracottatech.License;
import com.terracottatech.licensing.client.LicenseEntityClientService;
import com.terracottatech.licensing.client.LicenseInstaller;
import com.terracottatech.licensing.common.LicenseCreator;
import com.terracottatech.licensing.server.LicenseEntityService;
import com.terracottatech.licensing.services.LicenseServiceProvider;

import org.junit.After;
import org.junit.Before;
import org.terracotta.catalog.SystemCatalog;
import org.terracotta.catalog.client.CatalogClientService;
import org.terracotta.catalog.server.CatalogServerEntityService;
import org.terracotta.client.message.tracker.OOOMessageHandlerProvider;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.lease.LeaseAcquirerClientService;
import org.terracotta.lease.LeaseAcquirerServerService;
import org.terracotta.management.entity.nms.agent.client.NmsAgentEntityClientService;
import org.terracotta.management.entity.nms.agent.server.NmsAgentEntityServerService;
import org.terracotta.management.service.monitoring.MonitoringServiceProvider;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddresses;

public abstract class AbstractEnterprisePassthroughTest {

  private final List<PassthroughClusterControl> stripes = new ArrayList<>();

  @Before
  public final void _before() {
    for (String stripeName : provideStripeNames()) {
      PassthroughClusterControl stripe = PassthroughTestHelpers.createMultiServerStripe(stripeName, provideStripeSize(), getDefaultServerInitializer());
      stripes.add(stripe);
    }

    for (PassthroughClusterControl stripe : stripes) {
      stripe.startAllServers();
    }

    LicenseInstaller licenseInstaller = new LicenseInstaller();
    License testLicense =  LicenseCreator.createTestLicense();
    Properties properties = new Properties();
    if (provideStripeSize() == 1) {
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, EnterprisePassthroughConnectionService.SS_SCHEME);
    } else {
      properties.setProperty(ConnectionPropertyNames.CONNECTION_TYPE, EnterprisePassthroughConnectionService.MS_SCHEME);
    }
    String[] servers = provideStripeNames().toArray(new String[provideStripeNames().size()]);
    licenseInstaller.installLicense(getInetSocketAddresses(servers), testLicense, properties);
  }

  @After
  public final void _after() throws Exception {
    for (PassthroughClusterControl stripe : stripes) {
      stripe.tearDown();
    }
  }

  protected void restartActives() throws Exception {
    stripes.forEach(PassthroughClusterControl::terminateActive);
    stripes.forEach(PassthroughClusterControl::startAllServers);
    for (PassthroughClusterControl stripe : stripes) {
      stripe.waitForActive();
    }
  }

  protected final List<PassthroughClusterControl> getListOfClusterControls() {
    return stripes;
  }

  protected final URI buildClusterUri() {
    try {
      StringBuilder uriSb = new StringBuilder();
      if (provideStripeNames().size() == 1) {
        uriSb.append(EnterprisePassthroughConnectionService.SS_SCHEME);
      } else {
        uriSb.append(EnterprisePassthroughConnectionService.MS_SCHEME);
      }
      uriSb.append("://");

      for (String stripeName : provideStripeNames()) {
        uriSb.append(stripeName).append(",");
      }
      uriSb.deleteCharAt(uriSb.length() - 1);
      return new URI(uriSb.toString());
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }

  protected abstract List<String> provideStripeNames();

  protected int provideStripeSize() {
    return provideStripeNames().size();
  }

  protected PassthroughTestHelpers.ServerInitializer provideExtraServerInitializer() {
    return server -> {};
  }

  private PassthroughTestHelpers.ServerInitializer getDefaultServerInitializer() {
    return server -> {
      server.registerClientEntityService(new CatalogClientService());
      server.registerServerEntityService(new CatalogServerEntityService());
      server.registerClientEntityService(new LicenseEntityClientService());
      server.registerServerEntityService(new LicenseEntityService());
      server.registerServiceProvider(new LicenseServiceProvider(), null);
      server.registerClientEntityService(new LeaseAcquirerClientService());
      server.registerServerEntityService(new LeaseAcquirerServerService());
      server.registerServiceProvider(new OOOMessageHandlerProvider(), null);

      // management is now already there by default in both Ehcache EE and TcStore EE
      server.registerServiceProvider(new MonitoringServiceProvider(), null);
      server.registerServerEntityService(new NmsAgentEntityServerService());
      server.registerClientEntityService(new NmsAgentEntityClientService());

      provideExtraServerInitializer().registerServicesForServer(server);
    };
  }

  protected SystemCatalog fetchSystemCatalog(Connection connection) {
    try {
      EntityRef<SystemCatalog, Object, Object> entityRef = connection.getEntityRef(SystemCatalog.class, SystemCatalog.VERSION, SystemCatalog.ENTITY_NAME);
      return entityRef.fetchEntity(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
