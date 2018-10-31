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
package com.terracottatech.testing.rules.fot;

import org.junit.Rule;

import com.terracottatech.testing.rules.EnterpriseCluster;
import com.terracottatech.testing.rules.EnterpriseExternalClusterBuilder;

import java.io.InputStream;
import java.util.Scanner;

public abstract class EnterpriseExternalClusterITBase {

  @Rule
  public final EnterpriseCluster CLUSTER = EnterpriseExternalClusterBuilder.newCluster(2)
      .withPlugins(getServiceConfig("persistent-cluster.xmlfrag")).withFailoverPriorityVoterCount(failoverPriorityVoterCount()).build();

  protected abstract int failoverPriorityVoterCount();

  static String getServiceConfig(String configName) {
    InputStream resource = EnterpriseExternalClusterITBase.class.getResourceAsStream("/" + configName);
    try (Scanner scanner = new Scanner(resource, "UTF-8").useDelimiter("\\A")) {
      return scanner.next();
    }
  }

}
