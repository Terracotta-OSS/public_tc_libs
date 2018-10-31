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
package com.terracottatech.sovereign.common.utils;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Timed watcher class, useful for keeping an eye on state during long runs.
 *
 * @author cschanck
 */
public final class TimedWatcher {
  private static Timer timer = new Timer("Watcher", true);

  private TimedWatcher() {
  }

  public static TimerTask watch(int millisecondsInterval, final Runnable r) {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        r.run();
      }
    };
    timer.scheduleAtFixedRate(task, millisecondsInterval, millisecondsInterval);
    return task;
  }
}
