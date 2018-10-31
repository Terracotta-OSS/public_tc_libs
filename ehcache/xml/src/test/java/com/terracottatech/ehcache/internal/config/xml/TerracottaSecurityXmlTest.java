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
package com.terracottatech.ehcache.internal.config.xml;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import com.terracottatech.connection.EnterpriseConnectionPropertyNames;

import java.nio.file.Paths;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class TerracottaSecurityXmlTest {
  @Test
  public void testXmlConfigWithSecurityRootDirectoryConfig() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/cluster-security.xml"));
    ClusteringServiceConfiguration config = ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class,
                                                                              xmlConfiguration.getServiceCreationConfigurations());
    assertThat(config.getProperties().getProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY),
               is(Paths.get("/path/to/security-root-directory").toString()));
  }

  @Test
  public void testXmlConfigWithoutSecurityRootDirectoryConfig() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/cluster-ha.xml"));
    ClusteringServiceConfiguration config = ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class,
                                                                              xmlConfiguration.getServiceCreationConfigurations());
    assertThat(config.getProperties().getProperty(EnterpriseConnectionPropertyNames.SECURITY_ROOT_DIRECTORY), nullValue());
  }
}
