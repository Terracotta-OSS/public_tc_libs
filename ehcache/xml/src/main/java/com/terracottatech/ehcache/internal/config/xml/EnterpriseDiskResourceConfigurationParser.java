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

import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.terracottatech.ehcache.disk.internal.config.FastRestartStoreResourcePoolImpl;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.NAMESPACE_URI;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.TERRACOTTA_NAMESPACE_PREFIX;
import static com.terracottatech.ehcache.internal.config.xml.EnterpriseClusteredCacheConstants.XML_SCHEMA_URI;

/**
 * Parser to parse XML extensions for enterprise disk specific functionality.
 *
 * @author RKAV
 */
public class EnterpriseDiskResourceConfigurationParser extends BaseConfigParser<ResourcePool> implements CacheResourceConfigurationParser {

  // Constants identifying XML extensions for enterprise
  private static final String FRS_ELEMENT_NAME = "disk-restartable";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA_URI.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE_URI;
  }

  @Override
  public ResourcePool parseResourceConfiguration(final Element fragment) {
    final String elementName = fragment.getLocalName();
    if (FRS_ELEMENT_NAME.equals(elementName)) {
      final String unitValue = fragment.getAttribute("unit").toUpperCase();
      final MemoryUnit sizeUnits = MemoryUnit.valueOf(unitValue);
      final String sizeValue = fragment.getFirstChild().getNodeValue();
      final long size = Long.parseLong(sizeValue);
      return new FastRestartStoreResourcePoolImpl(size, sizeUnits);
    }

    return null;
  }

  @Override
  public Element unparseResourcePool(final ResourcePool resourcePool) {
    return unparseConfig(resourcePool);
  }

  @Override
  protected Element createRootElement(Document doc, ResourcePool resourcePool) {
    Element rootElement = null;
    if (FastRestartStoreResourcePoolImpl.class == resourcePool.getClass()) {
      FastRestartStoreResourcePoolImpl restartStoreResourcePool = (FastRestartStoreResourcePoolImpl) resourcePool;
      rootElement = doc.createElementNS(NAMESPACE_URI.toString(), TERRACOTTA_NAMESPACE_PREFIX + FRS_ELEMENT_NAME);
      rootElement.setAttribute("unit", restartStoreResourcePool.getUnit().toString());
      rootElement.setTextContent(String.valueOf(restartStoreResourcePool.getSize()));
    }
    return rootElement;
  }

  @Override
  public Set<Class<? extends ResourcePool>> getResourceTypes() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(FastRestartStoreResourcePoolImpl.class)));
  }

}
