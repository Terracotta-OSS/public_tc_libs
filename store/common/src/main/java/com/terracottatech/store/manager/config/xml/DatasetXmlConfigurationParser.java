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

import com.terracottatech.store.configuration.PersistentStorageType;
import com.terracottatech.store.manager.DatasetManagerConfiguration.DatasetInfo;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.terracottatech.store.Type;
import com.terracottatech.store.configuration.AbstractDatasetConfiguration;
import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DatasetConfiguration;
import com.terracottatech.store.configuration.DatasetConfigurationBuilder;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.indexing.IndexSettings;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Map;
import java.util.Optional;

import static com.terracottatech.store.manager.config.xml.AbstractXmlConfigurationParser.DATASET_NAMESPACE_PREFIX;
import static com.terracottatech.store.manager.xml.util.Misc.getTimeUnit;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getAttribute;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getNodeValue;
import static com.terracottatech.store.manager.xml.util.XmlUtils.getOptionalAttribute;

class DatasetXmlConfigurationParser {
  private static final String OFFHEAP_RESOURCE_TAG = "offheap-resource";
  private static final String DISK_RESOURCE_TAG = "disk-resource";
  private static final String INDEXES_TAG = "indexes";
  private static final String INDEX_TAG = "index";
  private static final String CELL_DEFINITION_TAG = "cell-definition";
  private static final String INDEX_TYPE_TAG = "type";
  private static final String DURABILITY_EVENTUAL_TAG = "durability-eventual";
  private static final String DURABILITY_EVERY_MUTATION_TAG = "durability-every-mutation";
  private static final String DURABILITY_TIMED_TAG = "durability-timed";
  private static final String ADVANCED_CONFIG_TAG = "advanced";
  private static final String CONCURRENCY_HINT_TAG = "concurrency-hint";

  private static final String DATASET_NAME_ATTRIBUTE = "name";
  private static final String DATASET_NAME_CELL_TYPE_ATTRIBUTE = "key-type";

  private static final String CELL_DEFINITION_NAME_ATTRIBUTE = "name";
  private static final String CELL_DEFINITION_TYPE_ATTRIBUTE = "type";
  private static final String TIME_UNIT_ATTRIBUTE = "unit";
  private static final String STORAGE_TYPE_ATTRIBUTE = "storage-type";


  static ConfigurationHolder parse(Node parentNode, DatasetConfigurationBuilder datasetConfigurationBuilder) {
    NodeList childNodes = parentNode.getChildNodes();
    String offheapResourceName;

    String datasetName = getAttribute(parentNode, DATASET_NAME_ATTRIBUTE);
    String keyType = getAttribute(parentNode, DATASET_NAME_CELL_TYPE_ATTRIBUTE);

    for (int i = 0; i < childNodes.getLength(); i++) {
      Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case OFFHEAP_RESOURCE_TAG: {
            offheapResourceName = getNodeValue(childNode);
            datasetConfigurationBuilder = datasetConfigurationBuilder.offheap(offheapResourceName);
            break;
          }

          case DISK_RESOURCE_TAG: {
            String diskResourceName = getNodeValue(childNode);
            Optional<String> storageTypeAttribute = getOptionalAttribute(childNode, STORAGE_TYPE_ATTRIBUTE);
            if (storageTypeAttribute.isPresent()) {
              PersistentStorageType storageType = PersistentStorageType.shortNameToType(storageTypeAttribute.get());
              datasetConfigurationBuilder = datasetConfigurationBuilder.disk(diskResourceName, storageType);
            } else {
              datasetConfigurationBuilder = datasetConfigurationBuilder.disk(diskResourceName);
            }
            break;
          }

          case INDEXES_TAG: {
            datasetConfigurationBuilder = parseIndexes(childNode, datasetConfigurationBuilder);
            break;
          }

          case DURABILITY_EVENTUAL_TAG: {
            datasetConfigurationBuilder = datasetConfigurationBuilder.durabilityEventual();
            break;
          }

          case DURABILITY_EVERY_MUTATION_TAG: {
            datasetConfigurationBuilder = datasetConfigurationBuilder.durabilityEveryMutation();
            break;
          }

          case DURABILITY_TIMED_TAG: {
            String durabilityTimed = getNodeValue(childNode);
            String timeUnit = getAttribute(childNode, TIME_UNIT_ATTRIBUTE);

            datasetConfigurationBuilder = datasetConfigurationBuilder.durabilityTimed(Long.parseLong(durabilityTimed),
                                                                                      getTimeUnit(timeUnit));
            break;
          }

          case ADVANCED_CONFIG_TAG: {
            datasetConfigurationBuilder = parseAdvancedConfig(childNode, datasetConfigurationBuilder);
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }

    return new ConfigurationHolder(datasetName, createDatasetInfo(keyType, datasetConfigurationBuilder.build()));
  }

  static void unparse(Document document,
                      Element datasetElement,
                      String datasetName,
                      Type<?> keyType,
                      DatasetConfiguration datasetConfiguration) {
    datasetElement.setAttribute(DATASET_NAME_ATTRIBUTE, datasetName);
    datasetElement.setAttribute(DATASET_NAME_CELL_TYPE_ATTRIBUTE, keyType.asEnum().toString());

    String offheapResource = datasetConfiguration.getOffheapResource();
    Element offheapResourceElement = document.createElement(namespacePrefixedElement(OFFHEAP_RESOURCE_TAG));
    offheapResourceElement.appendChild(document.createTextNode(offheapResource));
    datasetElement.appendChild(offheapResourceElement);

    datasetConfiguration.getDiskResource().ifPresent((diskResource) -> {
      Element diskResourceElement = document.createElement(namespacePrefixedElement(DISK_RESOURCE_TAG));
      datasetConfiguration.getPersistentStorageType().ifPresent((storageType ->
          diskResourceElement.setAttribute(STORAGE_TYPE_ATTRIBUTE, storageType.getShortName())));
      diskResourceElement.appendChild(document.createTextNode(diskResource));
      datasetElement.appendChild(diskResourceElement);
    });

    Map<CellDefinition<?>, IndexSettings> indexes = datasetConfiguration.getIndexes();
    if (indexes != null && !indexes.isEmpty()) {
      Element indexesElement = document.createElement(namespacePrefixedElement(INDEXES_TAG));
      datasetElement.appendChild(indexesElement);
      for (Map.Entry<CellDefinition<?>, IndexSettings> entry : indexes.entrySet()) {
        CellDefinition<?> cellDefinition = entry.getKey();
        IndexSettings indexSettings = entry.getValue();
        Element indexElement = document.createElement(namespacePrefixedElement(INDEX_TAG));
        Element cellDefinitionElement = document.createElement(namespacePrefixedElement(CELL_DEFINITION_TAG));
        cellDefinitionElement.setAttribute(CELL_DEFINITION_NAME_ATTRIBUTE, cellDefinition.name());
        cellDefinitionElement.setAttribute(CELL_DEFINITION_TYPE_ATTRIBUTE, cellDefinition.type().asEnum().toString());
        indexElement.appendChild(cellDefinitionElement);

        Element indexTypeElement = document.createElement(namespacePrefixedElement(INDEX_TYPE_TAG));
        indexTypeElement.appendChild(document.createTextNode(indexSettings.toString()));
        indexElement.appendChild(indexTypeElement);

        indexesElement.appendChild(indexElement);
      }
      datasetElement.appendChild(indexesElement);
    }

    datasetConfiguration.getDiskDurability().ifPresent((diskDurability) -> {
      switch (diskDurability.getDurabilityEnum()) {
        case EVENTUAL: {
          Element diskDurabilityElement = document.createElement(namespacePrefixedElement(DURABILITY_EVENTUAL_TAG));
          datasetElement.appendChild(diskDurabilityElement);
          break;
        }

        case EVERY_MUTATION: {
          Element diskDurabilityElement = document.createElement(namespacePrefixedElement(DURABILITY_EVERY_MUTATION_TAG));
          datasetElement.appendChild(diskDurabilityElement);
          break;
        }

        case TIMED: {
          BaseDiskDurability.Timed timed = (BaseDiskDurability.Timed)diskDurability;
          Element diskDurabilityElement = document.createElement(namespacePrefixedElement(DURABILITY_TIMED_TAG));
          diskDurabilityElement.setAttribute(TIME_UNIT_ATTRIBUTE, "MILLIS");
          diskDurabilityElement.appendChild(document.createTextNode(String.valueOf(timed.getMillisDuration())));
          datasetElement.appendChild(diskDurabilityElement);
          break;
        }

        default: throw new IllegalArgumentException("Unknown DiskDurabilityEum: " + diskDurability.getDurabilityEnum());
      }
    });

    ((AbstractDatasetConfiguration)datasetConfiguration).getConcurrencyHint().ifPresent((hint) -> {
      Element advancedElement = document.createElement(namespacePrefixedElement(ADVANCED_CONFIG_TAG));
      Element concurrencyElement = document.createElement(namespacePrefixedElement(CONCURRENCY_HINT_TAG));
      concurrencyElement.appendChild(document.createTextNode(hint.toString()));
      advancedElement.appendChild(concurrencyElement);
      datasetElement.appendChild(advancedElement);
    });
  }

  private static String namespacePrefixedElement(String element) {
    return DATASET_NAMESPACE_PREFIX + ":" + element;
  }

  private static DatasetConfigurationBuilder parseAdvancedConfig(Node parentNode,
                                                                 DatasetConfigurationBuilder datasetConfigurationBuilder) {
    NodeList childNodes = parentNode.getChildNodes();

    for (int i = 0; i < childNodes.getLength(); i++) {
      Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case CONCURRENCY_HINT_TAG: {
            String concurrencyHint = getNodeValue(childNode);
            datasetConfigurationBuilder = datasetConfigurationBuilder.advanced()
                                                                     .concurrencyHint(Integer.parseInt(concurrencyHint));
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }
    return datasetConfigurationBuilder;
  }

  private static DatasetConfigurationBuilder parseIndexes(Node parentNode,
                                                          DatasetConfigurationBuilder datasetConfigurationBuilder) {
    NodeList childNodes = parentNode.getChildNodes();

    for (int i = 0; i < childNodes.getLength(); i++) {
      Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case INDEX_TAG: {
            datasetConfigurationBuilder = parseIndex(childNode, datasetConfigurationBuilder);
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }

    return datasetConfigurationBuilder;
  }

  private static DatasetConfigurationBuilder parseIndex(Node parentNode, DatasetConfigurationBuilder
      datasetConfigurationBuilder) {
    NodeList childNodes = parentNode.getChildNodes();
    CellDefinition<?> cellDefinition = null;
    IndexSettings indexSettings = null;

    for (int j = 0; j < childNodes.getLength(); j++) {
      Node childNode = childNodes.item(j);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case CELL_DEFINITION_TAG: {
            String cellName = getAttribute(childNode, CELL_DEFINITION_NAME_ATTRIBUTE);
            Type<?> cellType = getCellType(getAttribute(childNode, CELL_DEFINITION_TYPE_ATTRIBUTE));

            cellDefinition = CellDefinition.define(cellName, cellType);
            break;
          }

          case INDEX_TYPE_TAG: {
            indexSettings = getIndexSettings(getNodeValue(childNode));
            break;
          }

          default: throw new AssertionError("Unknown nodes must not exists as XML configuration was already " +
                                            "validated");
        }
      }
    }
    return datasetConfigurationBuilder.index(cellDefinition, indexSettings);
  }

  static class ConfigurationHolder {
    private final String datasetName;
    private final DatasetInfo<?> datasetInfo;

    ConfigurationHolder(String datasetName, DatasetInfo<?> datasetInfo) {
      this.datasetName = datasetName;
      this.datasetInfo = datasetInfo;
    }

    public String getDatasetName() {
      return datasetName;
    }

    public DatasetInfo<?> getDatasetInfo() {
      return datasetInfo;
    }
  }

  static DatasetInfo<?> createDatasetInfo(String keyType, DatasetConfiguration datasetConfiguration) {
    switch (keyType) {
      case "BOOL": return new DatasetInfo<>(Type.BOOL, datasetConfiguration);
      case "CHAR": return new DatasetInfo<>(Type.CHAR, datasetConfiguration);
      case "INT": return new DatasetInfo<>(Type.INT, datasetConfiguration);
      case "LONG": return new DatasetInfo<>(Type.LONG, datasetConfiguration);
      case "DOUBLE": return new DatasetInfo<>(Type.DOUBLE, datasetConfiguration);
      case "STRING": return new DatasetInfo<>(Type.STRING, datasetConfiguration);
      default: throw new IllegalArgumentException("Unexpected key type: " + keyType);
    }
  }

  static Type<?> getKeyType(String cellType) {
    switch (cellType) {
      case "BOOL": return Type.BOOL;
      case "CHAR": return Type.CHAR;
      case "INT": return Type.INT;
      case "LONG": return Type.LONG;
      case "DOUBLE": return Type.DOUBLE;
      case "STRING": return Type.STRING;
      default: throw new IllegalArgumentException("Unexpected key type: " + cellType);
    }
  }

  static Type<?> getCellType(String cellType) {
    switch (cellType) {
      case "BOOL": return Type.BOOL;
      case "CHAR": return Type.CHAR;
      case "INT": return Type.INT;
      case "LONG": return Type.LONG;
      case "DOUBLE": return Type.DOUBLE;
      case "STRING": return Type.STRING;
      case "BYTES": return Type.BYTES;
      default: throw new IllegalArgumentException("Unexpected cell type: " + cellType);
    }
  }

  static IndexSettings getIndexSettings(String indexType) {
    switch (indexType) {
      case "BTREE": return IndexSettings.BTREE;
      default: throw new IllegalArgumentException("Unexpected index type: " + indexType);
    }
  }
}
