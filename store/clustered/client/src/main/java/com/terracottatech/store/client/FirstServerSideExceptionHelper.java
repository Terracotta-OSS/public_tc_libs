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
package com.terracottatech.store.client;

import org.terracotta.exception.ServerSideExceptionWrapper;

import java.util.regex.Pattern;

// TODO - get rid of this once https://github.com/Terracotta-OSS/terracotta-apis/issues/208 is resolved
public class FirstServerSideExceptionHelper {
  private static final Pattern prefixPattern = buildPrefixPattern();

  public static String getMessage(Exception e) {
    Throwable lowestException = getFirstServerSideException(e);
    String message = lowestException.getMessage();
    return stripClassname(message);
  }

  private static Throwable getFirstServerSideException(Throwable exception) {
    while (true) {
      if (exception instanceof ServerSideExceptionWrapper) {
        return exception;
      }

      Throwable cause = exception.getCause();

      if (cause == null) {
        return exception;
      }

      exception = cause;
    }
  }

  private static String stripClassname(String message) {
    return prefixPattern.matcher(message).replaceFirst("");
  }

  private static Pattern buildPrefixPattern() {
    // We won't match every legal FQCN with this pattern
    // but we avoid more false positives.

    String packageName = "[a-z]([a-z0-9_])*";
    String className = "[A-Z][A-Za-z0-9_$]*";

    String packageNameWithDot = packageName + "\\.";
    String multiplePackageName = "(" + packageNameWithDot + ")+";
    String fullyQualifiedClassName = multiplePackageName + className;

    String prefix = fullyQualifiedClassName + ": ";

    return Pattern.compile(prefix);
  }
}
