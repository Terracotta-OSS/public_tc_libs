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
package com.terracottatech.testing.delay;

import org.terracotta.entity.EntityMessage;
import org.terracotta.entity.EntityResponse;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterDelay implements DelayControl {
  private final String name;
  private final List<StripeDelay> stripeDelays;

  public ClusterDelay(String name, int stripeCount) {
    this.name = name;
    stripeDelays = IntStream.range(0, stripeCount)
            .mapToObj(i -> new StripeDelay(name, i))
            .collect(Collectors.toList());
  }

  /**
   * @param stripeIndex zero based
   */
  public StripeDelay getStripeDelay(int stripeIndex) {
    return stripeDelays.get(stripeIndex);
  }

  @Override
  public void noDelay() {
    stripeDelays.forEach(DelayControl::noDelay);
  }

  @Override
  public void setDefaultMessageDelay(long defaultMessageDelay) {
    stripeDelays.forEach(stripeDelay -> stripeDelay.setDefaultMessageDelay(defaultMessageDelay));
  }

  @Override
  public void setDefaultResponseDelay(long defaultResponseDelay) {
    stripeDelays.forEach(stripeDelay -> stripeDelay.setDefaultResponseDelay(defaultResponseDelay));
  }

  @Override
  public void setMessageDelay(Class<? extends EntityMessage> messageClass, long delay) {
    stripeDelays.forEach(stripeDelay -> stripeDelay.setMessageDelay(messageClass, delay));
  }

  @Override
  public void setResponseDelay(Class<? extends EntityResponse> responseClass, long delay) {
    stripeDelays.forEach(stripeDelay -> stripeDelay.setResponseDelay(responseClass, delay));
  }

  @Override
  public String toString() {
    return "ClusterDelay{ name: " + name + ", stripeDelays: " + stripeDelays + "}";
  }
}
