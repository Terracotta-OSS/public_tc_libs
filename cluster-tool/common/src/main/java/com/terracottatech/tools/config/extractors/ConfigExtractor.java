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
package com.terracottatech.tools.config.extractors;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.terracottatech.tools.config.Stripe;

import java.util.ArrayList;
import java.util.List;

public abstract class ConfigExtractor {

  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    ArrayList<Stripe.Config<?>> result = new ArrayList<>();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node item = nodeList.item(i);
      String value = item.getTextContent().trim();
      String name = item.getAttributes().getNamedItem(getAttributeName()).getNodeValue().trim();
      result.add(new Stripe.Config<>(getConfigType(), name, value));
    }
    return result;
  }

  public abstract String getXPathExpression();

  protected abstract Stripe.ConfigType getConfigType();

  protected abstract String getAttributeName();
}
