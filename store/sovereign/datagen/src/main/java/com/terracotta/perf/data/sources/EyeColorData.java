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
package com.terracotta.perf.data.sources;

import com.terracotta.perf.data.EyeColor;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Provides access to data about eye color distribution.
 *
 * @author Clifford W. Johnson
 */
public final class EyeColorData {

  /**
   * Eye color distribution.  These figures are "extrapolated" from
   * <a href="http://www.statisticbrain.com/eye-color-distribution-percentages/">Eye Color Distribution Percentages</a>
   * into the colors named in {@link EyeColor}.
   */
  public static final List<EyeColorWeight> EYE_COLOR_DISTRIBUTION;
  static {
    EYE_COLOR_DISTRIBUTION = Collections.unmodifiableList(
        new EyeColorWeight.Builder()
            .weight(EyeColor.BROWN, 41.0F)
            .weight(EyeColor.BLUE, 16.0F)
            .weight(EyeColor.GREY, 16.0F)
            .weight(EyeColor.HAZEL, 15.0F)
            .weight(EyeColor.AMBER, 6.0F)
            .weight(EyeColor.GREEN, 6.0F)
            .build());
  }

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private EyeColorData() {
  }

  /**
   * Returns an eye color based on the distribution in {@link #EYE_COLOR_DISTRIBUTION}.
   *
   * @param rnd the {@code Random} instance to use for eye color selection
   *
   * @return an eye color
   */
  public static EyeColor chooseEyeColor(final Random rnd) {
    final float choice = 100.0F * rnd.nextFloat();
    for (final EyeColorWeight colorWeight : EYE_COLOR_DISTRIBUTION) {
      if (choice <= colorWeight.getCumulativeWeight()) {
        return colorWeight.getValue();
      }
    }
    return EyeColor.BROWN;
  }

  /**
   * Describes the distribution weight of an {@link EyeColor EyeColor}.
   */
  private static final class EyeColorWeight extends WeightedValue<EyeColor> {
    private EyeColorWeight(final EyeColor eyeColor, final float weight) {
      super(eyeColor, weight);
    }

    private static final class Builder extends WeightedValue.Builder<EyeColor, EyeColorWeight> {
      @Override
      protected EyeColorWeight newInstance(final EyeColor value, final float weight) {
        return new EyeColorWeight(value, weight);
      }
    }
  }
}
