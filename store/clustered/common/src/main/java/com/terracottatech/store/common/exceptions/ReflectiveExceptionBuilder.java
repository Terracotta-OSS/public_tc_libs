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
package com.terracottatech.store.common.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectiveExceptionBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectiveExceptionBuilder.class);

  public static Throwable buildThrowable(String throwableClassName, String message, Throwable cause) {
    try {
      Class<?> throwableClass = Class.forName(throwableClassName, true, ReflectiveExceptionBuilder.class.getClassLoader());
      return buildThrowable(throwableClass, message, cause);
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Exception class not found : {}", throwableClassName, e);
      return new UnavailableException(message, cause, throwableClassName);
    }
  }

  public static Throwable buildThrowable(Class<?> throwableClass, String message, Throwable cause) {
    Throwable t = null;
    Exception ex = null;

    if (cause != null) {
      if (message != null) {
        try {
          Constructor<?> ctor = throwableClass.getConstructor(String.class, Throwable.class);
          t = (Throwable) ctor.newInstance(message, cause);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
          try {
            Constructor<?> ctor = throwableClass.getConstructor(String.class);
            t = (Throwable) ctor.newInstance(message);
            t.initCause(cause);
          } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e2) {
            try {
              Constructor<?> ctor = throwableClass.getConstructor(Throwable.class);
              t = (Throwable) ctor.newInstance(cause);
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e3) {
              ex = e3;
            }
          }
        }
      } else {
        try {
          Constructor<?> ctor = throwableClass.getConstructor(Throwable.class);
          t = (Throwable) ctor.newInstance(cause);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
          try {
            Constructor<?> ctor = throwableClass.getConstructor();
            t = (Throwable) ctor.newInstance();
            t.initCause(cause);
          } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e2) {
            ex = e2;
          }
        }
      }
    } else {
      if (message != null) {
        try {
          Constructor<?> ctor = throwableClass.getConstructor(String.class);
          t = (Throwable) ctor.newInstance(message);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
          try {
            Constructor<?> ctor = throwableClass.getConstructor(Object.class);
            t = (Throwable) ctor.newInstance(message);
          } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e2) {
            ex = e2;
          }
        }
      } else {
        try {
          Constructor<?> ctor = throwableClass.getConstructor();
          t = (Throwable) ctor.newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e1) {
          ex = e1;
        }
      }
    }

    if (ex != null) {
      LOGGER.debug("Failed to instantiate throwable of class {} with message '{}' and cause {}", throwableClass, message, cause, ex);
      return new UnavailableException(message, cause, throwableClass.getName());
    }

    return t;
  }
}
