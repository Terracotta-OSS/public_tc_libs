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

package com.terracottatech.store.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public class WhiteListObjectInputStream extends ObjectInputStream {

  private List<Pattern> whiteListedPatterns;

  public WhiteListObjectInputStream(InputStream in, List<Class<?>> whiteListedClasses) throws IOException {
    super(in);
    whiteListedPatterns = whiteListedClasses.stream()
        .map(Class::getName)
        .map(Pattern::quote)
        .map(Pattern::compile)
        .collect(toList());
  }

  @Override
  protected Class<?> resolveClass(ObjectStreamClass serialInput) throws IOException, ClassNotFoundException {
    final String name = serialInput.getName();

    if (whiteListedPatterns.stream()
            .noneMatch(pattern -> pattern.matcher(name).matches()))
    {
      String msg = "Class deserialization of " + name + " blocked by whitelist.";
      InvalidClassException exc = new InvalidClassException(msg);
      throw exc;
    }

    return super.resolveClass(serialInput);
  }
}

