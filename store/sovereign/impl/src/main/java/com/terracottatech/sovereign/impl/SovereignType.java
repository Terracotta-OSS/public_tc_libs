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
package com.terracottatech.sovereign.impl;

import com.terracottatech.store.Type;

/**
 * @author mscott
 */
public enum SovereignType {
  BOOLEAN(Boolean.class, Type.BOOL, true), CHAR(Character.class, Type.CHAR, true),
  STRING(String.class, Type.STRING, false), INT(Integer.class, Type.INT, true),
  LONG(Long.class, Type.LONG, true), DOUBLE(Double.class, Type.DOUBLE, true),
  BYTES(byte[].class, Type.BYTES, false);

  private final Class<?> jdkType;
  private final Type<?> storeType;
  private boolean isFixedSize;

  SovereignType(Class<?> type, Type<?> storeType, boolean isFixed) {
    jdkType = type;
    this.storeType = storeType;
    this.isFixedSize = isFixed;
  }

  private boolean assignable(Class<?> type) {
    return this.jdkType != null && this.jdkType.isAssignableFrom(type);
  }

  public Class<?> getJDKType() {
    return jdkType;
  }

  public Type<?> getNevadaType() {
    return storeType;
  }

  public static SovereignType forJDKType(Class<?> type) {
    for (SovereignType st : values()) {
      if (st.assignable(type)) {
        return st;
      }
    }
    return null;
  }

  public static SovereignType forType(Type<?> type) {
    for (SovereignType st : values()) {
      if (st.assignable(type.getJDKType())) {
        return st;
      }
    }
    return null;
  }

  public boolean isFixedSize() {
    return isFixedSize;
  }

}
