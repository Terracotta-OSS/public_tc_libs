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

package com.terracottatech.tool;

import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static org.junit.Assert.assertThat;

public final class WaitForAssert {

  private WaitForAssert() {}

  public static <T> EventualAssertion assertThatEventually(Supplier<T> value, Matcher<? super T> matcher) {
    return new EventualAssertion().and(value, matcher);
  }


  public static void assertThatCleanly(Callable<?> task, Duration timeout) throws TimeoutException {
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try {
      waitOrTimeout(() -> {
        try {
          task.call();
          return true;
        } catch (Throwable t) {
          failure.set(t);
          return false;
        }
      }, timeout);
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    } catch (TimeoutException e) {
      ofNullable(failure.get()).ifPresent(e::addSuppressed);
      throw e;
    }
  }

  public static final class EventualAssertion {

    private final Collection<Runnable> assertions = new ArrayList<>();

    public <T> EventualAssertion and(Supplier<T> value, Matcher<? super T> matcher) {
      assertions.add(() -> assertThat(value.get(), matcher));
      return this;
    }

    public void within(Duration timeout) throws TimeoutException {
      assertThatCleanly(() -> {
        assertions.forEach(Runnable::run);
        return null;
      }, timeout);
    }
  }

  private static void waitOrTimeout(BooleanSupplier condition, Duration timeout) throws TimeoutException, InterruptedException {
    Instant threshold = now().plus(timeout);
    Duration sleep = Duration.ofMillis(50);
    do {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(sleep.toMillis());
      sleep = sleep.multipliedBy(2);
    } while (now().isBefore(threshold));
    throw new TimeoutException();
  }
}
