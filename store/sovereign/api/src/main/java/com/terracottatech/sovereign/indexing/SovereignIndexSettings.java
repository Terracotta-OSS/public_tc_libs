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
package com.terracottatech.sovereign.indexing;

import java.io.Serializable;

/**
 * Created by cschanck
 */
@SuppressWarnings("serial")
public interface SovereignIndexSettings extends Serializable {

  boolean isSorted();

  boolean isUnique();

  static SovereignIndexSettings btree() {
    return BTREE;
  }

  static SovereignIndexSettings hash() {
    return HASH;
  }

  static SovereignIndexSettings BTREE = new BTREESovereignIndexSettings();

  class BTREESovereignIndexSettings implements SovereignIndexSettings {
    private static final long serialVersionUID = 2899841782308321688L;

    @Override
    public boolean isSorted() {
      return true;
    }

    @Override
    public boolean isUnique() {
      return false;
    }

    @Override
    public String toString() {
      return "BTREE";
    }

    @Override
    public int hashCode() {
      int ret = toString().hashCode();
      ret = ret + (isSorted() ? 100 : 50);
      ret = ret + (isUnique() ? 1 : 0);
      return ret;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || o.getClass() != getClass()) {
        return false;
      }

      SovereignIndexSettings that = (SovereignIndexSettings) o;
      return that.toString().equals(toString()) &&
             that.isSorted() == isSorted() &&
             that.isUnique() == isUnique();
    }
  };

  static SovereignIndexSettings HASH = new HASHSovereignIndexSettings();

  class HASHSovereignIndexSettings implements SovereignIndexSettings {
    private static final long serialVersionUID = -289941782308321688L;

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public boolean isUnique() {
      return true;
    }

    @Override
    public String toString() {
      return "HASH";
    }

    @Override
    public int hashCode() {
      int ret = toString().hashCode();
      ret = ret + (isSorted() ? 100 : 50);
      ret = ret + (isUnique() ? 1 : 0);
      return ret;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || o.getClass() != getClass()) {
        return false;
      }

      SovereignIndexSettings that = (SovereignIndexSettings) o;
      return that.toString().equals(toString()) &&
             that.isSorted() == isSorted() &&
             that.isUnique() == isUnique();
    }
  };
}
