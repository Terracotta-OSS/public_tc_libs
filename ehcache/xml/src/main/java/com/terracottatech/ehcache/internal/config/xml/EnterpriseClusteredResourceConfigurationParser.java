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

import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.xml.ClusteredResourceConfigurationParser;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.terracottatech.ehcache.clustered.client.config.builders.ClusteredRestartableResourcePoolBuilder;
import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredDedicatedResourcePoolImpl;
import com.terracottatech.ehcache.clustered.client.internal.config.EnterpriseClusteredSharedResourcePoolImpl;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.NAMESPACE_URI;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.TERRACOTTA_NAMESPACE_PREFIX;

public class EnterpriseClusteredResourceConfigurationParser extends ClusteredResourceConfigurationParser {

  private static final String CLUSTERED_RESTARTABLE_DEDICATED = "clustered-restartable-dedicated";
  private static final String CLUSTERED_RESTARTABLE_SHARED = "clustered-restartable-shared";
  private static final String SHARING = "sharing";
  private static final String FROM = "from";
  private static final String UNIT = "unit";
  private static final String DATA_PERCENT = "data-percent";

  @Override
  public URI getNamespace() {
    return NAMESPACE_URI;
  }

  @Override
  public ResourcePool parseResourceConfiguration(final Element fragment) {
    ResourcePool resourcePool = super.parseResourceConfig(fragment);
    if (resourcePool != null) {
      return resourcePool;
    }
    String elementName = fragment.getLocalName();
    if (CLUSTERED_RESTARTABLE_SHARED.equals(elementName)) {
      String sharing = fragment.getAttribute(SHARING);
      return ClusteredRestartableResourcePoolBuilder.clusteredRestartableShared(sharing);
    } else if (CLUSTERED_RESTARTABLE_DEDICATED.equals(elementName)) {
      // 'from' attribute is optional on 'clustered-restartable-dedicated' element
      final Attr fromAttr = fragment.getAttributeNode(FROM);
      final String from = (fromAttr == null ? null : fromAttr.getValue());

      final String unitValue = fragment.getAttribute(UNIT).toUpperCase();
      final MemoryUnit sizeUnits;
      try {
        sizeUnits = MemoryUnit.valueOf(unitValue);
      } catch (IllegalArgumentException iae) {
        throw new XmlConfigurationException(String.format("XML configuration element <%s> 'unit' attribute '%s' is not valid", elementName, unitValue), iae);
      }

      final String sizeValue = fragment.getFirstChild().getNodeValue();
      final long size;
      try {
        size = Long.parseLong(sizeValue);
      } catch (NumberFormatException nfe) {
        throw new XmlConfigurationException(String.format("XML configuration element <%s> value '%s' is not valid", elementName, sizeValue), nfe);
      }

      Attr dataPercentAttr = fragment.getAttributeNode(DATA_PERCENT);
      if (dataPercentAttr == null) {
        return ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(from, size, sizeUnits);
      } else {
        int valuesPercent = Integer.parseInt(dataPercentAttr.getValue());
        return ClusteredRestartableResourcePoolBuilder.clusteredRestartableDedicated(from, size, sizeUnits, valuesPercent);
      }
    }

    return null;
  }

  @Override
  public Element unparseResourcePool(ResourcePool resourcePool) {
    return unparseConfig(resourcePool);
  }

  @Override
  protected Element createRootElement(Document doc, ResourcePool resourcePool) {
    Element rootElement = super.createRootElement(doc, resourcePool);
    if (rootElement != null) {
      return rootElement;
    }

    if (EnterpriseClusteredSharedResourcePoolImpl.class == resourcePool.getClass()) {
      EnterpriseClusteredSharedResourcePoolImpl enterpriseClusteredSharedResourcePool = (EnterpriseClusteredSharedResourcePoolImpl) resourcePool;
      rootElement = doc.createElementNS(NAMESPACE_URI.toString(), TERRACOTTA_NAMESPACE_PREFIX + CLUSTERED_RESTARTABLE_SHARED);
      rootElement.setAttribute(SHARING, enterpriseClusteredSharedResourcePool.getSharedResourcePool());
    } else if (EnterpriseClusteredDedicatedResourcePoolImpl.class == resourcePool.getClass()) {
      EnterpriseClusteredDedicatedResourcePoolImpl enterpriseClusteredDedicatedResourcePool = (EnterpriseClusteredDedicatedResourcePoolImpl) resourcePool;
      rootElement = doc.createElementNS(NAMESPACE_URI.toString(), TERRACOTTA_NAMESPACE_PREFIX + CLUSTERED_RESTARTABLE_DEDICATED);
      if (enterpriseClusteredDedicatedResourcePool.getFromResource() != null) {
        rootElement.setAttribute(FROM, enterpriseClusteredDedicatedResourcePool.getFromResource());
      }
      rootElement.setAttribute(UNIT, enterpriseClusteredDedicatedResourcePool.getUnit().toString());
      rootElement.setTextContent(String.valueOf(enterpriseClusteredDedicatedResourcePool.getSize()));
      if (enterpriseClusteredDedicatedResourcePool.getDataPercent() != -1) {
        rootElement.setAttribute(DATA_PERCENT, String.valueOf(enterpriseClusteredDedicatedResourcePool.getDataPercent()));
      }
    }
    return rootElement;
  }

  @Override
  public Set<Class<? extends ResourcePool>> getResourceTypes() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ClusteredResourcePoolImpl.class,
        DedicatedClusteredResourcePoolImpl.class, SharedClusteredResourcePoolImpl.class,
        EnterpriseClusteredDedicatedResourcePoolImpl.class, EnterpriseClusteredSharedResourcePoolImpl.class)));
  }

}
