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

import static com.terracottatech.tools.config.Stripe.ConfigType.BACKUP_RESTORE;

public class BackupRestoreConfigExtractor extends ConfigExtractor {

  private static final String BACKUP_RESTORE_EXP = "/tc-config/plugins/service/backup-restore";
  private static final String BACKUP_LOCATION_ELEMENT_NAME = "backup-location";

  @Override
  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    List<Stripe.Config<?>> result = new ArrayList<>();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node.hasChildNodes()) {
        NodeList childNodes = node.getChildNodes();
        for (int j = 0; j < childNodes.getLength(); j++) {
          Node child = childNodes.item(j);
          if (child.getNodeName().endsWith(BACKUP_LOCATION_ELEMENT_NAME)) {
            result.add(new Stripe.Config<>(getConfigType(), BACKUP_LOCATION_ELEMENT_NAME, true));
          }
        }
      }
    }
    return result;
  }

  @Override
  public String getXPathExpression() {
    return BACKUP_RESTORE_EXP;
  }

  @Override
  protected Stripe.ConfigType getConfigType() {
    return BACKUP_RESTORE;
  }

  @Override
  protected String getAttributeName() {
    throw new UnsupportedOperationException("Can't use backup-restore config extractor that way");
  }
}
