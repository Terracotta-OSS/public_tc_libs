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

package com.terracottatech.sovereign;

import org.junit.Test;

import java.util.function.BiFunction;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests default methods in the {@link VersionLimitStrategy} interface.
 *
 * @author Clifford W. Johnson
 */
public class VersionLimitStrategyTest {
  @SuppressWarnings("rawtypes")
  @Test
  public void testGetTimeReferenceFilter() throws Exception {
    final VersionLimitStrategy<?> strategy = new VersionLimitStrategy() {
      private static final long serialVersionUID = 5025974096562804740L;

      @Override
      public Class<?> type() {
        return null;
      }
    };

    final BiFunction<?, ?, VersionLimitStrategy.Retention> filter = strategy.getTimeReferenceFilter();
    assertThat(filter, is(not(nullValue())));
    assertThat(filter.apply(null, null), is(VersionLimitStrategy.Retention.FINISH));
  }
}
