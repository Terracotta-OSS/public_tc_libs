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

package com.terracottatech.ehcache.clustered.server.services.frs;

import com.tc.classloader.CommonComponent;

/**
 * Utility methods to compose and decompose identifiers.
 *
 * @author RKAV
 */
@CommonComponent
public final class IdentifierComposer {
  private static final String DIVIDER = ":";

  public static String composedTierIdToTierId(String tierManagerId, String composedTierId) {
    String expectedPrefix = tierManagerId + DIVIDER;
    int index = composedTierId.indexOf(tierManagerId);
    if(index != -1) {
      return composedTierId.substring(index + expectedPrefix.length());
    }
    return composedTierId;
  }

  public static String tierIdToComposedTierId(String tierManagerId, String tierId) {
    return tierManagerId + DIVIDER + tierId;
  }

  public static String composedRepoIdToRepoId(String tierManagerId, String tierId, String composedRepoId) {
    String expectedPrefix = tierManagerId + DIVIDER + tierId + DIVIDER;
    int index = composedRepoId.indexOf(expectedPrefix);
    if(index != -1) {
      return composedRepoId.substring(index + expectedPrefix.length());
    }
    return composedRepoId;
  }

  public static String repoIdToComposedRepoId(String tierManagerId, String tierId, String repoId) {
    return tierManagerId + DIVIDER + tierId + DIVIDER + repoId;
  }
}