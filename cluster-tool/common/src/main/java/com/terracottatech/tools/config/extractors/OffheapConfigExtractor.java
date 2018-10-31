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

import static com.terracottatech.tools.config.Stripe.ConfigType.OFFHEAP;

public class OffheapConfigExtractor extends ConfigExtractor {

  private static final String NAME_ATTR = "name";
  private static final String UNIT_ATTR = "unit";

  private static final String OFFHEAP_EXP = "/tc-config/plugins/config/offheap-resources/resource";

  @Override
  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    ArrayList<Stripe.Config<?>> result = new ArrayList<Stripe.Config<?>>();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node item = nodeList.item(i);
      String value = item.getTextContent().trim();
      String name = item.getAttributes().getNamedItem(getAttributeName()).getNodeValue().trim();
      String unit = item.getAttributes().getNamedItem(UNIT_ATTR).getNodeValue().trim();
      long val = MemoryUnit.valueOf(unit.toUpperCase()).toBytes(Long.parseLong(value));
      result.add(new Stripe.Config<Long>(getConfigType(), name, val));
    }
    return result;
  }

  @Override
  public String getXPathExpression() {
    return OFFHEAP_EXP;
  }

  @Override
  protected Stripe.ConfigType getConfigType() {
    return OFFHEAP;
  }

  @Override
  protected String getAttributeName() {
    return NAME_ATTR;
  }

  enum MemoryUnit {
    B(0),
    KB(10),
    MB(20),
    GB(30),
    TB(40),
    PB(50);

    private final int bitShift;
    private final long mask;

    MemoryUnit(int bitShift) {
      this.bitShift = bitShift;
      this.mask = -1L << (63 - bitShift);
    }

    public long toBytes(long quantity) {
      if (bitShift == 0) {
        return quantity;
      }

      if (quantity == Long.MIN_VALUE) {
        throw new IllegalArgumentException("Byte count is too large: " + quantity + this);
      }

      if (quantity < 0) {
        return -1 * toBytes(-1 * quantity);
      }

      if (quantity == 0) {
        return 0;
      }

      if ((quantity & mask) != 0) {
        throw new IllegalArgumentException("Byte count is too large: " + quantity + this);
      }

      return quantity << bitShift;
    }
  }
}
