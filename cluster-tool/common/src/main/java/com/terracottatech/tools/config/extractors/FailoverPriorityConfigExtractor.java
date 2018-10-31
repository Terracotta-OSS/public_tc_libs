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

import com.terracottatech.tools.config.Stripe;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

import static com.terracottatech.tools.config.Stripe.ConfigType.FAILOVER_PRIORITY;

public class FailoverPriorityConfigExtractor extends ConfigExtractor {

  public static final String AVAILABILITY_ELEMENT = "availability";
  public static final String CONSISTENCY_ELEMENT = "consistency";
  private static final String VOTER_ELEMENT = "voter";
  private static final String COUNT_ATTR = "count";
  private static final String FAILOVER_PRIORITY_EXP = "/tc-config/failover-priority";

  @Override
  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    List<Stripe.Config<?>> result = new ArrayList<Stripe.Config<?>>();
    if (nodeList.getLength() > 0) {
      Node node = nodeList.item(0);
      if (node.hasChildNodes()) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
          Node child = childNodes.item(i);
          if (child.getNodeName().endsWith(AVAILABILITY_ELEMENT)) {
            result.add(new Stripe.Config<>(getConfigType(), AVAILABILITY_ELEMENT, "-1"));
          } else if (child.getNodeName().endsWith(CONSISTENCY_ELEMENT)) {
            String count = "0";
            NodeList consistencyChildNodes = child.getChildNodes();
            for (int j = 0; j < consistencyChildNodes.getLength(); j++) {
              Node consistencyChild = consistencyChildNodes.item(i);
              if (consistencyChild.getNodeName().endsWith(VOTER_ELEMENT)) {
                count = consistencyChild.getAttributes().getNamedItem(COUNT_ATTR).getNodeValue();
              }
            }
            result.add(new Stripe.Config<>(getConfigType(), CONSISTENCY_ELEMENT, count));
          }
        }
      }
    } else {
      result.add(new Stripe.Config<>(getConfigType(), AVAILABILITY_ELEMENT, "-1"));
    }
    return result;
  }

  @Override
  public String getXPathExpression() {
    return FAILOVER_PRIORITY_EXP;
  }

  @Override
  protected Stripe.ConfigType getConfigType() {
    return FAILOVER_PRIORITY;
  }

  @Override
  protected String getAttributeName() {
    throw new UnsupportedOperationException("No attributes supported");
  }
}
