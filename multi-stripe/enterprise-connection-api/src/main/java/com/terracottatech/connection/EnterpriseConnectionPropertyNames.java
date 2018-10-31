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

package com.terracottatech.connection;

/**
 * The {@code EnterpriseConnectionPropertyNames} interface specifies connection properties for enterprise features, e.g.,
 * a secure connection.
 */
public interface EnterpriseConnectionPropertyNames {

  /**
   * <p>Property to specify a security root directory, which must be passed to {@code ConnectionFactory.connect} to use
   * SSL/TLS-enabled connections.</p>
   *
   * <p>The security root directory must contain following two sub-directories with required files as explained below:</p>
   *
   * <ol>
   *  <li>trusted-authority - contains a truststore with certificates trusted by this client</li>
   *  <li>identity - contains a keystore with this client's private key and the certificate</li>
   * </ol>
   *
   * <p>Supported keystore types: "jks"</p>
   */
  String SECURITY_ROOT_DIRECTORY = "security.root.directory";
}
