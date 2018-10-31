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
package com.terracottatech.store.manager.xml.parser;

import com.terracottatech.store.manager.DatasetManagerConfiguration;
import com.terracottatech.store.manager.DatasetManagerConfigurationException;
import com.terracottatech.store.manager.DatasetManagerConfigurationParser;
import com.terracottatech.store.manager.xml.exception.XmlConfigurationException;
import com.terracottatech.store.manager.xml.util.XmlUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class XmlConfigurationParser implements DatasetManagerConfigurationParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(XmlConfigurationParser.class);

  public DatasetManagerConfiguration parse(URL configurationURL) throws DatasetManagerConfigurationException {
    try {
      LOGGER.info("Trying to load DatasetManager XML configuration from " + configurationURL);
      Map<XmlConfigurationExtensionParser.Type, XmlConfigurationExtensionParser> parsers = new HashMap<>();
      for (XmlConfigurationExtensionParser.Type type : XmlConfigurationExtensionParser.Type.values()) {
        try {
          XmlConfigurationExtensionParser parser = XmlConfigurationExtensionParser.getXmlConfigurationExtensionParser(type);
          parsers.put(type, parser);
        } catch (Exception ignored) {}
      }

      if (parsers.values().isEmpty()) {
        throw new RuntimeException("Couldn't find any XmlConfigurationExtensionParser implementations");
      }

      Document document = XmlUtils.getValidatedDocument(configurationURL.openStream(),
                                                        parsers.values().stream().map(XmlConfigurationExtensionParser::getSchema).collect(Collectors.toList()));
      Element rootElement = document.getDocumentElement();
      String rootElementName = rootElement.getLocalName();

      DatasetManagerConfiguration datasetManagerConfiguration =
          parsers.get(XmlConfigurationExtensionParser.Type.valueOf(rootElementName.toUpperCase())).parse(document);

      LOGGER.info("Successfully loaded the XML configuration from " + configurationURL);
      return datasetManagerConfiguration;
    } catch (Exception e) {
      String errMsg = "Error while loading the XML configuration from " + configurationURL;
      LOGGER.error(errMsg, e);
      throw new XmlConfigurationException(errMsg, e);
    }
  }

  @Override
  public String unparse(DatasetManagerConfiguration datasetManagerConfiguration) {
    try {
      Document translatedDocument =
          XmlConfigurationExtensionParser.getXmlConfigurationExtensionParser(XmlConfigurationExtensionParser.Type.valueOf(datasetManagerConfiguration.getType().name().toUpperCase()))
                                         .unparse(datasetManagerConfiguration);
      return XmlUtils.convertToString(translatedDocument);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
