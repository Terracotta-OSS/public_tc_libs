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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;

/**
 * Reflectively analyses methods of given objects or classes.
 */
public class MethodAnalyser {

  private final List<?> parameters;

  public MethodAnalyser(List<?> parameters) {
    this.parameters = parameters;
  }

  public Map<Method, ?> mapMethods(Object foo, Class<?> as) {
    return of(as.getMethods())
            .filter(method -> !method.isBridge())
            .filter(method -> foo != null ^ Modifier.isStatic(method.getModifiers()))
            .filter(method -> allSuperInterfaces(method.getReturnType())
                    .anyMatch(i -> i.isAnnotationPresent(FunctionalInterface.class))
                    || Collector.class.isAssignableFrom(method.getReturnType()))
            .sorted(comparing(Method::getParameterCount).thenComparing(MethodAnalyser::methodString))
            .collect(toMap(identity(), method -> invoke(foo, method)));
  }

  private Object invoke(Object foo, Method method) {
    return generateParameters(method.getParameterTypes())
            .flatMap(params -> invokeWithParameters(foo, method, params))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing parameters for " + method));
  }

  private static Stream<?> invokeWithParameters(Object foo, Method method, List<Object> params) {
    try {
      return of(method.invoke(foo, params.toArray()));
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (Throwable t) {
      //try to find better parameter
      return empty();
    }
  }

  private Stream<List<Object>> generateParameters(Class<?>[] parameterTypes) {
    if (parameterTypes.length == 0) {
      return of(emptyList());
    } else {
      return generateCandidates(parameterTypes[0])
              .flatMap(cand -> generateParameters(copyOfRange(parameterTypes, 1, parameterTypes.length))
                      .map(sublist -> prepend(cand, sublist)));
    }
  }

  private static List<Object> prepend(Object cand, List<Object> sublist) {
    return concat(of(cand), sublist.stream()).collect(toList());
  }

  private static Stream<Class<?>> allSuperInterfaces(Class<?> type) {
    Stream.Builder<Class<?>> superInterfaces = Stream.builder();
    Deque<Class<?>> queue = new LinkedList<>();
    queue.push(type);
    while (!queue.isEmpty()) {
      Class<?> i = queue.pop();
      if (i.isInterface()) {
        superInterfaces.add(i);
      } else {
        ofNullable(i.getSuperclass()).ifPresent(queue::add);
      }
      queue.addAll(asList(i.getInterfaces()));
    }
    return superInterfaces.build();
  }

  private Stream<?> generateCandidates(Class<?> clazz) {
    if (clazz.isPrimitive()) {
      if (Integer.TYPE.equals(clazz)) {
        return of(42);
      } else if (Long.TYPE.equals(clazz)) {
        return of(42L);
      } else if (Double.TYPE.equals(clazz)) {
        return of(42.0);
      } else {
        throw new AssertionError(clazz);
      }
    } else {
      return parameters.stream().filter(clazz::isInstance);
    }
  }

  public static String methodString(Method key) {
    return key.getName() + of(key.getParameterTypes())
            .map(Class::getSimpleName)
            .collect(joining(", ", "(", ")"));
  }

  public boolean ignoreParameter(Object returnValue) {
    return !parameters.contains(returnValue);
  }
}
