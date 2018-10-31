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


package com.terracottatech.store.common.messages.stream;

import com.terracottatech.store.intrinsics.Intrinsic;
import com.terracottatech.tool.MethodAnalyser;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.terracottatech.store.common.messages.stream.ElementValue.ValueType;
import static org.junit.Assert.assertThat;

abstract class ReturnTypeCoverageTest<I extends Intrinsic> {

  private final MethodAnalyser analyser;

  private final Class<?> intrinsicClass;

  ReturnTypeCoverageTest(Class<?> intrinsicClass, List<?> parameters) {
    this.intrinsicClass = intrinsicClass;
    this.analyser = new MethodAnalyser(parameters);
  }

  <T> void testCompleteness(T foo, Class<T> as) {
    recurse(foo, as, new HashSet<>());
  }

  private void recurse(Object foo, Class<?> as, Set<Class<?>> inspected) {
    analyser.mapMethods(foo, as).forEach((method, returnValue) -> {
      if (analyser.ignoreParameter(returnValue) && !inspected.contains(returnValue.getClass())) {
        if (returnValue instanceof Intrinsic) {
          if (intrinsicClass.isInstance(returnValue)) {
            @SuppressWarnings("unchecked")
            I function = (I) returnValue;
            checkIntrinsicFunction(function);
          }
          inspected.add(returnValue.getClass());
          recurse(returnValue, method.getReturnType(), inspected);
        }
      }
    });
  }

  private void checkIntrinsicFunction(I intrinsic) {
    Object result = apply(intrinsic);
    ValueType valueType = ValueType.forObject(result);
    assertThat(valueType, typeMatcher());
  }

  abstract Object apply(I function);

  abstract Matcher<ValueType> typeMatcher();
}
