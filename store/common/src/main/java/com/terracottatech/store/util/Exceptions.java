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
package com.terracottatech.store.util;

import java.util.concurrent.Callable;
import java.util.stream.Stream;

public class Exceptions {

  public static void suppress(Runnable... operations) {
    Stream.of(operations).reduce(Exceptions::composeRunnables).ifPresent(Runnable::run);
  }

  public static Runnable composeRunnables(Runnable a, Runnable b) {
    return () -> {
      try {
        a.run();
      } catch (Throwable e1) {
        try {
          b.run();
        } catch (Throwable e2) {
          try {
            e1.addSuppressed(e2);
          } catch (Throwable ignore) {}
        }
        throw e1;
      }
      b.run();
    };
  }

  public static Callable<Void> composeCallables(Callable<?> a, Callable<?> b) {
    return () -> {
      try {
        a.call();
      } catch (Throwable e1) {
        try {
          b.call();
        } catch (Throwable e2) {
          try {
            e1.addSuppressed(e2);
          } catch (Throwable ignore) {}
        }
        throw e1;
      }
      b.call();
      return null;
    };
  }
}
