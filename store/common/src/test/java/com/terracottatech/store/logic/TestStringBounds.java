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

package com.terracottatech.store.logic;


/**
 * Test String bounds.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestStringBounds extends TestBounds<String> {


  @Override
  protected Start<String>[] getStartsInAscendingOrder() {
    return new Start[]{
            Interval.<String>negativeInfinity(),

            new Start<>("A", Start.Inclusion.OPEN),
            new Start<>("A", Start.Inclusion.CLOSED),

            new Start<>("Z", Start.Inclusion.OPEN),
            new Start<>("Z", Start.Inclusion.CLOSED)
    };
  }

  @Override
  protected End<String>[] getEndsInAscendingOrder() {
    return new End[]{
            new End<>("A", End.Inclusion.CLOSED),
            new End<>("A", End.Inclusion.OPEN),

            new End<>("Z", End.Inclusion.CLOSED),
            new End<>("Z", End.Inclusion.OPEN),

            Interval.<String>positiveInfinity()
    };
  }
}
