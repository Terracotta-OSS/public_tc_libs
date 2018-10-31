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
package com.terracottatech.tools.clustertool.managers;

import com.terracottatech.tools.client.TopologyEntityProvider;
import com.terracottatech.tools.client.TopologyEntityProvider.ConnectionCloseableTopologyEntity;
import com.terracottatech.tools.clustertool.managers.DefaultDiagnosticManager.ConnectionCloseableDiagnosticsEntity;
import com.terracottatech.utilities.InetSocketAddressConvertor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.terracottatech.utilities.InetSocketAddressConvertor.getInetSocketAddress;

public class CommonEntityManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyManager.class);
  private static final TopologyEntityProvider TOPOLOGY_ENTITY_PROVIDER = new TopologyEntityProvider();
  private static final DiagnosticManager DIAGNOSTIC_MANAGER = new DefaultDiagnosticManager();

  public ConnectionCloseableDiagnosticsEntity getDiagnosticsEntity(String hostPort, String securityRootDirectory) {
    return DIAGNOSTIC_MANAGER.getEntity(getInetSocketAddress(hostPort), securityRootDirectory);
  }

  public ConnectionCloseableTopologyEntity getTopologyEntity(List<String> hostPortList, String securityRootDirectory) {
    List<InetSocketAddress> servers = hostPortList.stream()
        .map(InetSocketAddressConvertor::getInetSocketAddress)
        .collect(Collectors.toList());
    return getTopologyEntity(servers, securityRootDirectory);
  }

  public ConnectionCloseableTopologyEntity getTopologyEntity(Iterable<InetSocketAddress> servers, String securityRootDirectory) {
    return getTopologyEntity(servers, securityRootDirectory, null);
  }

  public ConnectionCloseableTopologyEntity getTopologyEntity(Iterable<InetSocketAddress> servers, String securityRootDirectory,
                                                      Properties connProperties) {
    return TOPOLOGY_ENTITY_PROVIDER.getEntity(servers, securityRootDirectory, connProperties);
  }

  public void closeEntities(Collection<? extends Closeable> closeableEntities) {
    closeableEntities.forEach(this::closeEntity);
  }

  public void closeEntity(Closeable closeableEntity) {
    if (closeableEntity != null) {
      try {
        closeableEntity.close();
      } catch (Exception e) {
        LOGGER.debug("Failed to close entity with exception: {}", e);
      }
    }
  }

  public void releaseTopologyEntities(Collection<ConnectionCloseableTopologyEntity> closeableTopologyEntities) {
    closeableTopologyEntities.forEach(this::releaseTopologyEntity);
  }

  public void releaseTopologyEntity(ConnectionCloseableTopologyEntity closeableTopologyEntity) {
    if (closeableTopologyEntity != null) {
      try {
        closeableTopologyEntity.release();
      } catch (Exception e) {
        LOGGER.debug("Failed to release entity with exception: {}", e);
      }
    }
  }
}
