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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.terracottatech.tools.config.Stripe.ConfigType.AUDIT_DIRECTORY;
import static com.terracottatech.tools.config.Stripe.ConfigType.AUTHENTICATION;
import static com.terracottatech.tools.config.Stripe.ConfigType.SECURITY_ROOT_DIRECTORY;
import static com.terracottatech.tools.config.Stripe.ConfigType.SSL_TLS;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST;
import static com.terracottatech.tools.config.Stripe.ConfigType.WHITELIST_DEPRECATED;

public class SecurityConfigExtractor extends ConfigExtractor {

  private static final String SECURITY_EXP = "/tc-config/plugins/service/security";
  private static final String CERTIFICATE_AUTHENTICATION = "certificate";
  private static final String FILE_AUTHENTICATION = "file";
  private static final String LDAP_AUTHENTICATION = "ldap";

  @Override
  public List<Stripe.Config<?>> extractConfigFromNodes(NodeList nodeList) {
    Set<Stripe.Config<?>> result = new HashSet<>();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node.hasChildNodes()) {
        NodeList childNodes = node.getChildNodes();
        for (int j = 0; j < childNodes.getLength(); j++) {
          Node item = childNodes.item(j);
          String nodeName = item.getNodeName();
          if (nodeName.endsWith(WHITELIST.getName())) {
            result.add(getWhitelistConfig());
          } else if (nodeName.endsWith(WHITELIST_DEPRECATED.getName())) {
            result.add(getDeprecatedWhitelistConfig());
          } else if (nodeName.endsWith(SSL_TLS.getName())) {
            result.add(getSslTlsConfig());
          } else if (nodeName.endsWith(SECURITY_ROOT_DIRECTORY.getName())) {
            result.add(getSecurityRootDirectoryConfig());
          } else if (nodeName.endsWith(AUDIT_DIRECTORY.getName())) {
            result.add(getAuditDirectoryConfig());
          } else if (nodeName.endsWith(AUTHENTICATION.getName())) {
            if (node.hasChildNodes()) {
              NodeList children = item.getChildNodes();
              for (int k = 0; k < children.getLength(); k++) {
                String name = children.item(k).getNodeName();
                if (name.endsWith(CERTIFICATE_AUTHENTICATION)) {
                  result.add(getAuthenticationConfig(CERTIFICATE_AUTHENTICATION));
                } else if (name.endsWith(FILE_AUTHENTICATION)) {
                  result.add(getAuthenticationConfig(FILE_AUTHENTICATION));
                } else if (name.endsWith(LDAP_AUTHENTICATION)) {
                  result.add(getAuthenticationConfig(LDAP_AUTHENTICATION));
                }
              }
            }
          }
        }
      }
    }
    return validateSecurityConfigs(result);
  }

  @Override
  public String getXPathExpression() {
    return SECURITY_EXP;
  }

  @Override
  protected Stripe.ConfigType getConfigType() {
    throw new UnsupportedOperationException("Can't use security extractor that way");
  }

  @Override
  protected String getAttributeName() {
    throw new UnsupportedOperationException("Can't use security extractor that way");
  }

  private List<Stripe.Config<?>> validateSecurityConfigs(Set<Stripe.Config<?>> stripeConfigs) {
    if (stripeConfigs.contains(getSecurityRootDirectoryConfig())) {
      if (stripeConfigs.contains(getDeprecatedWhitelistConfig())) {
        throw new IllegalArgumentException("<" + WHITELIST_DEPRECATED + "> tag is not supported with <"
                                           + SECURITY_ROOT_DIRECTORY + "> tag. Use <" + WHITELIST + "> tag instead");
      } else if (!hasSecurityRootDirectoryRelevantTag(stripeConfigs)) {
        throw new IllegalArgumentException("An <" + SSL_TLS + ">, a <" + WHITELIST + ">, or an <" + AUTHENTICATION + "> tag "
                                           + "must be provided with <" + SECURITY_ROOT_DIRECTORY + "> tag");
      } else if (stripeConfigs.contains(getAuthenticationConfig(CERTIFICATE_AUTHENTICATION)) && !stripeConfigs.contains(getSslTlsConfig())) {
        throw new IllegalArgumentException("An <" + SSL_TLS + "> tag must be provided with <" + CERTIFICATE_AUTHENTICATION + "> tag");
      }
    } else {
      if (stripeConfigs.contains(getSslTlsConfig())) {
        throw new IllegalArgumentException("A <" + SECURITY_ROOT_DIRECTORY + "> tag must be provided with <"
                                           + SSL_TLS + "> tag");
      } else if (stripeConfigs.contains(getWhitelistConfig())) {
        throw new IllegalArgumentException("A <" + SECURITY_ROOT_DIRECTORY + "> tag must be provided with <"
                                           + WHITELIST + "> tag");
      } else if (stripeConfigs.contains(getAuditDirectoryConfig()) && !stripeConfigs.contains(getDeprecatedWhitelistConfig())) {
        throw new IllegalArgumentException("A <" + SECURITY_ROOT_DIRECTORY + "> tag must be provided with <"
                                           + AUDIT_DIRECTORY + "> tag");
      } else if (stripeConfigs.contains(getAuthenticationConfig(CERTIFICATE_AUTHENTICATION)) ||
                 stripeConfigs.contains(getAuthenticationConfig(FILE_AUTHENTICATION)) ||
                 stripeConfigs.contains(getAuthenticationConfig(LDAP_AUTHENTICATION))) {
        throw new IllegalArgumentException("A <" + SECURITY_ROOT_DIRECTORY + "> tag must be provided with <"
                                           + AUTHENTICATION + "> tag");
      }
    }

    return new ArrayList<>(stripeConfigs);
  }

  private boolean hasSecurityRootDirectoryRelevantTag(Set<Stripe.Config<?>> stripeConfigs) {
    return stripeConfigs.contains(getSslTlsConfig()) ||
           stripeConfigs.contains(getWhitelistConfig()) ||
           stripeConfigs.contains(getAuthenticationConfig(CERTIFICATE_AUTHENTICATION)) ||
           stripeConfigs.contains(getAuthenticationConfig(FILE_AUTHENTICATION)) ||
           stripeConfigs.contains(getAuthenticationConfig(LDAP_AUTHENTICATION));
  }

  private Stripe.Config<Boolean> getWhitelistConfig() {
    return new Stripe.Config<>(WHITELIST, WHITELIST.getName(), true);
  }

  private Stripe.Config<Boolean> getDeprecatedWhitelistConfig() {
    return new Stripe.Config<>(WHITELIST_DEPRECATED, WHITELIST_DEPRECATED.getName(), true);
  }

  private Stripe.Config<Boolean> getSecurityRootDirectoryConfig() {
    return new Stripe.Config<>(SECURITY_ROOT_DIRECTORY, SECURITY_ROOT_DIRECTORY.getName(), true);
  }

  private Stripe.Config<Boolean> getAuditDirectoryConfig() {
    return new Stripe.Config<>(AUDIT_DIRECTORY, AUDIT_DIRECTORY.getName(), true);
  }

  private Stripe.Config<Boolean> getSslTlsConfig() {
    return new Stripe.Config<>(SSL_TLS, SSL_TLS.getName(), true);
  }

  private Stripe.Config<Boolean> getAuthenticationConfig(String name) {
    return new Stripe.Config<>(AUTHENTICATION, name, true);
  }
}
