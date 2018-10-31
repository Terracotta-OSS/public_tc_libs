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

import java.util.List;

import static com.terracottatech.tools.config.Stripe.ConfigType.DATA_DIRECTORIES;

public class DataRootConfigExtractor extends ConfigExtractor {

  private static final String DATA_ROOT_EXP = "/tc-config/plugins/config/data-directories/directory";
  private static final String NAME_ATTR = "name";
  private static final String USE_FOR_PLATFORM_ATTR = "use-for-platform";

  @Override
  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    List<Stripe.Config<?>> configs = super.extractConfigFromNodes(nodeList);
    if (nodeList.getLength() == 1) {
      configs.add(new Stripe.Config<>(getConfigType(), USE_FOR_PLATFORM_ATTR, "true"));
    } else {
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node item = nodeList.item(i);
        Node namedItem = item.getAttributes().getNamedItem(USE_FOR_PLATFORM_ATTR);
        if (namedItem != null) {
          String name = namedItem.getNodeValue().trim();
          if (Boolean.valueOf(name)) {
            configs.add(new Stripe.Config<>(getConfigType(), USE_FOR_PLATFORM_ATTR, "true"));
          }
        }
      }
    }
    return configs;
  }

  @Override
  public String getXPathExpression() {
    return DATA_ROOT_EXP;
  }

  @Override
  protected Stripe.ConfigType getConfigType() {
    return DATA_DIRECTORIES;
  }

  @Override
  protected String getAttributeName() {
    return NAME_ATTR;
  }
}
