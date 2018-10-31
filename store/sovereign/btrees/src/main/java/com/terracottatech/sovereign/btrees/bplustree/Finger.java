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
package com.terracottatech.sovereign.btrees.bplustree;

import java.util.LinkedList;

/**
 * Finger class, used to facilitate the implementation of a cursor.
 */
@SuppressWarnings("serial")
public class Finger extends LinkedList<TreeLocation> {

  public Finger(Finger f) {
    for (TreeLocation t : f) {
      add(new TreeLocation(t));
    }
  }

  public Finger() {
    super();
  }

  public boolean matched() {
    return (!isEmpty() && getLast().getCurrentIndex() >= 0 && getLast().getNode().isLeaf());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Finger/").append(size());
    sb.append(" ").append(super.toString());
    return sb.toString();
  }
}
