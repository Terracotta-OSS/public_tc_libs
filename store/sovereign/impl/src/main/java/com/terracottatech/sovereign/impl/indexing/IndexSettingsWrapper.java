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
package com.terracottatech.sovereign.impl.indexing;

import com.terracottatech.sovereign.indexing.SovereignIndexSettings;

/**
 * Representation of the configuration settings for an index.
 * <p/>
 * This type also exposes a set of static settings builders for the
 * supported index types.
 *
 * @author Chris Dennis
 */
@SuppressWarnings("serial")
public class IndexSettingsWrapper implements SovereignIndexSettings, Comparable<IndexSettingsWrapper> {

  private SovereignIndexSettings settings;

  public IndexSettingsWrapper(SovereignIndexSettings settings) {
    this.settings = settings;
  }

  public int hashCode() {
    int ret = isSorted() ? 100 : 50;
    ret = ret + (isUnique() ? 1 : 0);
    return ret;
  }

  public boolean equals(Object o) {
    if (o != null && o instanceof IndexSettingsWrapper) {
      IndexSettingsWrapper other = (IndexSettingsWrapper) o;
      return other.isSorted() == isSorted() && other.isUnique() == isUnique();
    }
    return false;
  }

  @Override
  public String toString() {
    return "IndexSettings{ Unique: " + isUnique() + " Sorted: " + isSorted() + " }";
  }

  @Override
  public int compareTo(IndexSettingsWrapper o) {
    return staticCompareTo(this, o);
  }

  public static int staticCompareTo(SovereignIndexSettings left, SovereignIndexSettings right) {
    // settings.
    if (left.isUnique() && !right.isUnique()) {
      return 1;
    }
    if (!left.isUnique() && right.isUnique()) {
      return -1;
    }
    if (left.isSorted() && !right.isSorted()) {
      return 1;
    }
    if (!left.isSorted() && right.isSorted()) {
      return -1;
    }
    return 0;
  }

  @Override
  public boolean isSorted() {
    return settings.isSorted();
  }

  @Override
  public boolean isUnique() {
    return settings.isUnique();
  }

  public SovereignIndexSettings getSettings() {
    return settings;
  }
}
