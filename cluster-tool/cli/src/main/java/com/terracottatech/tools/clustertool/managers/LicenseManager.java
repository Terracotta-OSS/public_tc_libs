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
package com.terracottatech.tools.clustertool.managers;

import com.terracottatech.License;
import com.terracottatech.tools.config.Cluster;

import java.util.List;


public interface LicenseManager {

  /**
   * Parse the license at the provided file path.
   * @param licenseFilePath
   * @return
   */
  License parse(String licenseFilePath);

  /**
   * Verify if the provided cluster specification is compatible with a previously installed license.
   * @param cluster
   */
  void ensureCompatibleLicense(Cluster cluster);

  /**
   * Verify if the license is compatible with the provided cluster specification.
   * @param cluster
   * @param license
   */
  void ensureCompatibleLicense(Cluster cluster, License license);

  /**
   * Install the license on all the stripes of the configured cluster.
   * @param clusterName
   * @param cluster
   * @param license
   * @throws Exception
   *
   * @return true if license was successfully installed, false it is equal to the previously installed license.
   */
  boolean installLicense(String clusterName, Cluster cluster, License license);
}
