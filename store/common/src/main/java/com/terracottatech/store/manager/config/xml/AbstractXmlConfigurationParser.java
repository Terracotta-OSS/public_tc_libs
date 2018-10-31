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

import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.DatasetManagerConfiguration.DatasetInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.terracottatech.store.configuration.DatasetConfigurationBuilder;

import com.terracottatech.store.manager.xml.exception.XmlConfigurationException;
import com.terracottatech.store.manager.xml.parser.XmlConfigurationExtensionParser;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import javax.xml.XMLConstants;

abstract class AbstractXmlConfigurationParser implements XmlConfigurationExtensionParser {

  private static final String DATASET_TAG = "dataset";
  static final String DATASET_NAMESPACE = "http://www.terracottatech.com/v1/terracotta/store";
  static final String DATASET_NAMESPACE_PREFIX = "tcs";

  private final Map<String, DatasetInfo<?>> datasets = new HashMap<>();
  private final Supplier<DatasetConfigurationBuilder> datasetConfigurationBuilderSupplier;

  AbstractXmlConfigurationParser(Supplier<DatasetConfigurationBuilder> datasetConfigurationBuilderSupplier) {
    this.datasetConfigurationBuilderSupplier = datasetConfigurationBuilderSupplier;
  }


  protected abstract DatasetManagerConfiguration build(Map<String, DatasetInfo<?>> datasets);

  protected abstract void parseInternal(Node childNode);

  @Override
  public DatasetManagerConfiguration parse(Document document) {
    Element element = document.getDocumentElement();
    NodeList childNodes = element.getChildNodes();
    for (int i = 0; i < childNodes.getLength(); i++) {
      final Node childNode = childNodes.item(i);
      if (childNode.getNodeType() == Node.ELEMENT_NODE) {
        switch (childNode.getLocalName()) {
          case DATASET_TAG: {
            parseDatasetConfiguration(childNode);
            break;
          }

          default: parseInternal(childNode);
        }
      }
    }

    return build(datasets);
  }

  protected abstract Document unparseInternal(DatasetManagerConfiguration configuration);

  @Override
  public Document unparse(DatasetManagerConfiguration datasetManagerConfiguration) {
    Document document = unparseInternal(datasetManagerConfiguration);
    unparseDatasetConfiguration(document, datasetManagerConfiguration.getDatasets());
    return document;
  }

  private void parseDatasetConfiguration(Node parentNode) {
    DatasetConfigurationBuilder datasetConfigurationBuilder = datasetConfigurationBuilderSupplier.get();
    DatasetXmlConfigurationParser.ConfigurationHolder configurationHolder = DatasetXmlConfigurationParser.parse(parentNode, datasetConfigurationBuilder);

    String datasetName = configurationHolder.getDatasetName();
    if (datasets.containsKey(datasetName)) {
      throw new XmlConfigurationException("Two datasets configured with the same name: " + datasetName);
    }

    datasets.put(datasetName, configurationHolder.getDatasetInfo());
  }

  private void unparseDatasetConfiguration(Document document, Map<String, DatasetInfo<?>> datasets) {
    for (Map.Entry<String, DatasetInfo<?>> entry : datasets.entrySet()) {
      String datasetName = entry.getKey();
      DatasetInfo<?> datasetInfo = entry.getValue();

      Element datasetElement = document.createElement(DATASET_TAG);
      DatasetXmlConfigurationParser.unparse(document, datasetElement, datasetName, datasetInfo.getType(), datasetInfo.getDatasetConfiguration());
      document.getFirstChild().appendChild(datasetElement);
    }
  }
}