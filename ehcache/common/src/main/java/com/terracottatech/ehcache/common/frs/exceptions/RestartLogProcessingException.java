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

/**
 * Generic failures while processing any of the fast restart logs.
 *
 * @author RKAV
 */
public class RestartLogProcessingException extends FastRestartStoreRuntimeException {

  private static final long serialVersionUID = 5033336791496186313L;

  private static final String BASE_MESSAGE = "Execution failure while processing Fast Restart Log %s";
  private static final String MESSAGE_EXTRA = "in Container %s at root location %s";

  public RestartLogProcessingException(File logDir, String message, Throwable cause) {
    super(message + ":" + convertRestartLogDirToString(BASE_MESSAGE, MESSAGE_EXTRA, logDir), cause);
  }
}