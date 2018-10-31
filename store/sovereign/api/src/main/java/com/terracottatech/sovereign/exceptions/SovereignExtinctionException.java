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
package com.terracottatech.sovereign.exceptions;

/**
 * @author cschanck
 **/
public class SovereignExtinctionException extends SovereignRuntimeException {
  private static final long serialVersionUID = 9160150883752041299L;

  public enum ExtinctionType {
    UNKNOWN("Unknown Failure"),
    RESOURCE_ALLOCATION("Resource Allocation Failure"),
    MEMORY_ALLOCATION("Memory Allocation Failure"),
    PERSISTENCE_ADD_FAILURE("Persistence (Add) Failure"),
    PERSISTENCE_MUTATE_FAILURE("Persistence (Mutate) Failure"),
    PERSISTENCE_DELETE_FAILURE("Persistence (Delete) Failure"),
    PERSISTENCE_FLUSH_FAILURE("Persistence (Flush) Failure");

    private final String niceName;

    String message(String arg) {
      return niceName + ": " + arg;
    }

    public SovereignExtinctionException exception(Throwable t) {
      return new SovereignExtinctionException(message(t.toString()), t);
    }

    public SovereignExtinctionException exception(String message, Throwable t) {
      throw new SovereignExtinctionException(message(message), t);
    }

    ExtinctionType(String niceName) {
      this.niceName = niceName;
    }
  }

  private SovereignExtinctionException(String message, Throwable cause) {
    super(message, cause);
  }
}
