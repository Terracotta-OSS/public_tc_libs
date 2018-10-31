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

package com.terracottatech.store.intrinsics.impl;

import com.terracottatech.store.intrinsics.IntrinsicType;

import java.util.Collections;

/**
 * Created by cdennis on 5/11/17.
 */
public class AlwaysTrue<T> extends LeafIntrinsicPredicate<T> {

  @SuppressWarnings("rawtypes")
  private static final AlwaysTrue TRUE = new AlwaysTrue<>();

  @SuppressWarnings("unchecked")
  public static <T> AlwaysTrue<T> alwaysTrue() {
    return TRUE;
  }

  private AlwaysTrue() {
    super(IntrinsicType.PREDICATE_ALWAYS_TRUE);
  }

  @Override
  public boolean test(T t) {
    return true;
  }

  @Override
  public String toString() {
    return "true";
  }
}
