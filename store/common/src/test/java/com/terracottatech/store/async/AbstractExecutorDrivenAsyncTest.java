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
package com.terracottatech.store.async;

import java.lang.reflect.Array;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractExecutorDrivenAsyncTest {

  protected static <CMO extends MO, MI, MO, DI, DO> void testDelegation(
          Function<DI, MI> subjectSupplier, Class<CMO> testOutType, Function<MI, MO> mainMethod,
          Class<DI> delgInType, Class<DO> delgOutType, Function<DI, DO> delgMethod,
          BiConsumer<CMO, DO> validation) {
    DI delgIn = createInstance(delgInType);
    DO delgOut = createInstance(delgOutType);
    if (!Void.TYPE.equals(delgOutType)) {
      when(delgMethod.apply(delgIn)).thenReturn(delgOut);
    }

    MO testOut = mainMethod.apply(subjectSupplier.apply(delgIn));

    delgMethod.apply(verify(delgIn));

    if (!Void.TYPE.equals(testOutType)) {
      assertThat(testOut, instanceOf(testOutType));
    }

    validation.accept(testOutType.cast(testOut), delgOut);
  }

  @SuppressWarnings("unchecked")
  protected static <U> U createInstance(Class<U> paramType) {
    if (Integer.TYPE.equals(paramType)) {
      return (U) Integer.valueOf(0);
    } else if (Long.TYPE.equals(paramType)) {
      return (U) Long.valueOf(0L);
    } else if (Double.TYPE.equals(paramType)) {
      return (U) Double.valueOf(0L);
    } else if (Void.TYPE.equals(paramType)) {
      return null;
    } else if (Integer.class.equals(paramType)) {
      return (U) Integer.valueOf(0);
    } else if (Long.class.equals(paramType)) {
      return (U) Long.valueOf(0L);
    } else if (Double.class.equals(paramType)) {
      return (U) Double.valueOf(0L);
    } else if (Boolean.class.equals(paramType)) {
      return (U) Boolean.FALSE;
    } else if (String.class.equals(paramType)) {
      return (U) "";
    } else if (Optional.class.equals(paramType)) {
      return (U) Optional.empty();
    } else if (OptionalInt.class.equals(paramType)) {
      return (U) OptionalInt.empty();
    } else if (OptionalLong.class.equals(paramType)) {
      return (U) OptionalLong.empty();
    } else if (OptionalDouble.class.equals(paramType)) {
      return (U) OptionalDouble.empty();
    } else if (paramType.isArray()) {
      return (U) Array.newInstance(paramType.getComponentType(), 0);
    } else {
      return mock(paramType);
    }
  }

  protected static <T> T retrieve(Future<T> f) {
    try {
      return f.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AssertionError(e);
    }
  }

  protected interface TriFunction<T, U, V, R> {
    R apply(T t, U u, V v);
  }

  protected interface QuadFunction<T, U, V, W, R> {
    R apply(T t, U u, V v, W w);
  }
}
