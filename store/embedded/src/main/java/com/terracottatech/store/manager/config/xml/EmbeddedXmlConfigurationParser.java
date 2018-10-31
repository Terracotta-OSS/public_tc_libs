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
package com.terracottatech.store.manager.config.xml;

import com.terracottatech.store.builder.DiskResource;
import com.terracottatech.store.configuration.MemoryUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.terracottatech.store.builder.EmbeddedDatasetConfigurationBuilder;
import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.EmbeddedDatasetManagerBuilder.FileMode;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfiguration.ResourceConfiguration;
import com.terracottatech.store.manager.config.EmbeddedDatasetManagerConfigurationBuilder;
import com.terracottatech.store.manager.xml.exception.XmlConfigurationException;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getAttribute;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getNodeValue;

public class EmbeddedXmlConfigurationParser extends AbstractXmlConfigurationParser {

  private static final URL SCHEMA = EmbeddedXmlConfigurationParser.class.getResource("/embedded.xsd");
  private static final String NAMESPACE = "http://www.terracottatech.com/v1/terracotta/store/embedded";

  private static final String OFFHEAP_RESOURCES_TAG = "offheap-resources";
  private static final String OFFHEAP_RESOURCE_TAG = "offheap-resource";
  private static final String DISK_RESOURCES_TAG = "disk-resources";
  private static final String DISK_RESOURCE_TAG = "disk-resource";

  private static final String OFFHEAP_RESOURCE_NAME_ATTRIBUTE = "name";
  private static final String OFFHEAP_RESOURCE_UNIT_ATTRIBUTE = "unit";
  private static final String DISK_RESOURCE_NAME_ATTRIBUTE = "name";
  private static final String DISK_RESOURCE_FILE_MODE_ATTRIBUTE = "file-mode";

  private EmbeddedDatasetManagerConfigurationBuilder builder = new EmbeddedDatasetManagerConfigurationBuilder();

  public EmbeddedXmlConfigurationParser() {
    super(EmbeddedDatasetConfigurationBuilder::new);
  }

  @Override
  protected DatasetManagerConfiguration build(Map<String, DatasetManagerConfiguration.DatasetInfo<?>> datasets) {
    return builder.datasetConfigurations(datasets).build();
  }

  @Override
  protected void parseInternal(Node childNode) {
    switch (childNode.getLocalName()) {
      case OFFHEAP_RESOURCES_TAG: {
        builder = parseOffheapResources(childNode, builder);
        break;
      }

      case DISK_RESOURCES_TAG: {
        builder = parseDiskResources(childNode, builder);
        break;
      }

      default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                        "validated");
    }
  }

  @Override
  public Document unparseInternal(DatasetManagerConfiguration datasetManagerConfiguration) {
    try {
      if (!(datasetManagerConfiguration instanceof EmbeddedDatasetManagerConfiguration)) {
        throw new IllegalArgumentException("Unexpected configuration type: " + datasetManagerConfiguration.getClass());
      }
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document document = builder.newDocument();
      Element documentElement = document.createElementNS(NAMESPACE, Type.EMBEDDED.name().toLowerCase());
      documentElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + DATASET_NAMESPACE_PREFIX, DATASET_NAMESPACE);
      document.appendChild(documentElement);
      ResourceConfiguration configuration = ((EmbeddedDatasetManagerConfiguration)datasetManagerConfiguration).getResourceConfiguration();
      Map<String, Long> offheapResourcesMap = configuration.getOffheapResources();
      if (!offheapResourcesMap.isEmpty()) {
        Element offheapResourcesElement = document.createElement(OFFHEAP_RESOURCES_TAG);
        documentElement.appendChild(offheapResourcesElement);
        for (Map.Entry<String, Long> entry : offheapResourcesMap.entrySet()) {
          String offheapResourceName = entry.getKey();
          Long offheapResourceSize = entry.getValue();
          Element offheapResourceElement = document.createElement(OFFHEAP_RESOURCE_TAG);
          offheapResourceElement.setAttribute(OFFHEAP_RESOURCE_NAME_ATTRIBUTE, offheapResourceName);
          offheapResourceElement.setAttribute(OFFHEAP_RESOURCE_UNIT_ATTRIBUTE, "B");
          offheapResourceElement.appendChild(document.createTextNode(offheapResourceSize.toString()));
          offheapResourceElement.setNodeValue(offheapResourceSize.toString());
          offheapResourcesElement.appendChild(offheapResourceElement);
        }
      }

      Map<String, DiskResource> diskResourceMap = configuration.getDiskResources();

      if (!diskResourceMap.isEmpty()) {
        Element diskResourcesElement = document.createElement(DISK_RESOURCES_TAG);
        documentElement.appendChild(diskResourcesElement);
        for (Map.Entry<String, DiskResource> entry : diskResourceMap.entrySet()) {
          String diskResourceName = entry.getKey();
          DiskResource diskResource = entry.getValue();
          Element diskResourceElement = document.createElement(DISK_RESOURCE_TAG);
          diskResourceElement.setAttribute(DISK_RESOURCE_NAME_ATTRIBUTE, diskResourceName);
          diskResourceElement.setAttribute(DISK_RESOURCE_FILE_MODE_ATTRIBUTE, diskResource.getFileMode().name());
          diskResourceElement.appendChild(document.createTextNode(diskResource.getDataRoot().toString()));
          diskResourcesElement.appendChild(diskResourceElement);
        }
      }

      return document;
    } catch (ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<Type> getSupportedTypes() {
    return Collections.singleton(Type.EMBEDDED);
  }

  @Override
  public URL getSchema() {
    return SCHEMA;
  }

  private static EmbeddedDatasetManagerConfigurationBuilder parseOffheapResources(Node parentNode,
                                                         EmbeddedDatasetManagerConfigurationBuilder builder) {
    NodeList childNodes = parentNode.getChildNodes();
    Set<String> offheapResourcesNames = new HashSet<>();

    for (int i = 0; i < childNodes.getLength(); i++) {
      final Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case OFFHEAP_RESOURCE_TAG: {
            String offheapResourceName = getAttribute(childNode, OFFHEAP_RESOURCE_NAME_ATTRIBUTE);
            if (offheapResourcesNames.contains(offheapResourceName)) {
              throw new XmlConfigurationException("Two offheap-resources configured with the same name: " +
                                                  offheapResourceName);
            }
            offheapResourcesNames.add(offheapResourceName);

            String offheapResourceUnit = getAttribute(childNode, OFFHEAP_RESOURCE_UNIT_ATTRIBUTE);
            String offheapResourceSize = getNodeValue(childNode);
            builder = builder.offheap(offheapResourceName, Long.parseLong(offheapResourceSize), getMemoryUnit(offheapResourceUnit));
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }

    return builder;
  }

  private static EmbeddedDatasetManagerConfigurationBuilder parseDiskResources(Node parentNode,
                                                              EmbeddedDatasetManagerConfigurationBuilder builder) {
    NodeList childNodes = parentNode.getChildNodes();
    Set<String> diskResourceNames = new HashSet<>();

    for (int i = 0; i < childNodes.getLength(); i++) {
      final Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case DISK_RESOURCE_TAG: {
            String diskResourceName = getAttribute(childNode, DISK_RESOURCE_NAME_ATTRIBUTE);
            if (diskResourceNames.contains(diskResourceName)) {
              throw new XmlConfigurationException("Two disk-resources configured with the same name: " +
                                                  diskResourceName);
            }
            diskResourceNames.add(diskResourceName);

            String fileMode = getAttribute(childNode, DISK_RESOURCE_FILE_MODE_ATTRIBUTE);
            String path = getNodeValue(childNode);
            builder = builder.disk(diskResourceName, Paths.get(path), FileMode.valueOf(fileMode));
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }

    return builder;
  }

  private static MemoryUnit getMemoryUnit(String memoryUnit) {
    switch (memoryUnit) {
      case "B": return MemoryUnit.B;
      case "KB": return MemoryUnit.KB;
      case "MB": return MemoryUnit.MB;
      case "GB": return MemoryUnit.GB;
      case "TB": return MemoryUnit.TB;
      case "PB": return MemoryUnit.PB;
      default: throw new IllegalArgumentException("Unexpected MemoryUnit: " + memoryUnit);
    }
  }
}
