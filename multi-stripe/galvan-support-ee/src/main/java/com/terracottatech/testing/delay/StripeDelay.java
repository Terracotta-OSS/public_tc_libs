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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class StripeDelay implements Delay, DelayControl {
  private final String name;
  private final int stripeIndex;
  private volatile long defaultMessageDelay;
  private volatile long defaultResponseDelay;
  private final Map<Class<?>, Long> messageDelays = new ConcurrentHashMap<>();
  private final Map<Class<?>, Long> responseDelays = new ConcurrentHashMap<>();

  public StripeDelay(String name, int stripeIndex) {
    this.name = name;
    this.stripeIndex = stripeIndex;
  }

  @Override
  public void noDelay() {
    this.defaultMessageDelay = 0L;
    this.defaultResponseDelay = 0L;
    messageDelays.clear();
    responseDelays.clear();
  }

  public void setDefaultMessageDelay(long defaultMessageDelay) {
    this.defaultMessageDelay = defaultMessageDelay;
  }

  public void setDefaultResponseDelay(long defaultResponseDelay) {
    this.defaultResponseDelay = defaultResponseDelay;
  }

  public void setMessageDelay(Class<? extends EntityMessage> messageClass, long delay) {
    messageDelays.put(messageClass, delay);
  }

  public void setResponseDelay(Class<? extends EntityResponse> responseClass, long delay) {
    responseDelays.put(responseClass, delay);
  }

  @Override
  public void clientToServerDelay(EntityMessage message) {
    Supplier<Long> delaySupplier = getDelay(messageDelays, message, () -> defaultMessageDelay);
    delay(delaySupplier);
  }

  @Override
  public void serverToClientDelay(EntityResponse response) {
    Supplier<Long> delaySupplier = getDelay(responseDelays, response, () -> defaultResponseDelay);
    delay(delaySupplier);
  }

  private void delay(Supplier<Long> delaySupplier) {
    long start = System.nanoTime();
    try {
      while (moreDelay(start, delaySupplier)) {
        Thread.sleep(100L);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean moreDelay(long start, Supplier<Long> delaySupplier) {
    long now = System.nanoTime();
    long nanosSoFar = now - start;
    long millisSoFar = TimeUnit.MILLISECONDS.convert(nanosSoFar, TimeUnit.NANOSECONDS);
    return millisSoFar < delaySupplier.get();
  }

  private Supplier<Long> getDelay(Map<Class<?>, Long> delays, Object object, Supplier<Long> defaultSupplier) {
    return () -> {
      Class<?> objectClass = object.getClass();

      for (Map.Entry<Class<?>, Long> delay : delays.entrySet()) {
        Class<?> delayClass = delay.getKey();
        if (delayClass.isAssignableFrom(objectClass)) {
          return delay.getValue();
        }
      }

      return defaultSupplier.get();
    };
  }

  @Override
  public String toString() {
    return "StripeDelay{ name: " + name + ", stripeIndex: " + stripeIndex +
            ", defaultMessageDelay: " + defaultMessageDelay + ", defaultResponseDelay: " + defaultResponseDelay +
            ", messageDelays: " + messageDelays + ", responseDelays: " + responseDelays + "}";
  }
}
