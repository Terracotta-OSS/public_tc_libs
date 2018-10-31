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
package com.terracottatech.ehcache.common.frs.exceptions;

import java.io.File;
import java.util.regex.Pattern;

/**
 * Base class for all Fast Restart Store exceptions
 *
 * @author RKAV
 */
abstract class FastRestartStoreRuntimeException extends RuntimeException {

  private static final long serialVersionUID = -8242978509341037970L;

  FastRestartStoreRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  static String convertRestartLogDirToString(String baseTemplate, String msgExtra, File logDir) {
    String logDirAsString = logDir.getAbsolutePath();
    String[] pathComponents = logDirAsString.split(Pattern.quote(File.separator));
    int len = pathComponents.length;
    return len > 3 ? String.format(baseTemplate + " " + msgExtra, pathComponents[len-1], pathComponents[len-2],
        pathComponents[len-3]) : String.format(baseTemplate, logDirAsString);
  }
}