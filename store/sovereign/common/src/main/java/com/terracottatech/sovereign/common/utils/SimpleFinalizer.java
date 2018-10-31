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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * <p>Class to replicate finalizers without the pain. You provide a MARKER object and
 * the actual OBJECT to be used to reclaim things. The MARKER cannot be referenced
 * from the OBJECT. When the marker is GC'd, the task provided will be run, passing
 * in the OBJECT.</p>
 * <p>It is implemented using per thread maps, which are keyed on the OBJ, so the OBJ
 * must implement a reasonable hashcode & equals contract.</p>
 *
 * @author cschanck
 **/
public class SimpleFinalizer<MARKER, OBJ> {

  public static final Logger LOGGER = LoggerFactory.getLogger(SimpleFinalizer.class);

  private final Consumer<OBJ> task;
  private final Timer timer;
  private final TimerTask timerTask;
  private final Map<OBJ, Wrapper<MARKER, OBJ>> map = new ConcurrentHashMap<>();
  private final ReferenceQueue<MARKER> refQueue = new ReferenceQueue<>();

  private static class Wrapper<M, OO> extends PhantomReference<M> {
    private final Map<OO, Wrapper<M, OO>> map;
    private final OO obj;

    public Wrapper(M marker, OO obj, Map<OO, Wrapper<M, OO>> map, ReferenceQueue<? super M> refQ) {
      super(marker, refQ);
      this.map = map;
      this.obj = obj;
    }

    public OO getObj() {
      return obj;
    }

    public void reclaim() {
      map.remove(obj);
    }
  }

  public SimpleFinalizer(Class<OBJ> type, Consumer<OBJ> task) {
    this(type, 1, TimeUnit.MINUTES, task);
  }

  public SimpleFinalizer(Class<OBJ> type, long period, TimeUnit unit, Consumer<OBJ> task) {
    assertObjectEqualityOf(type);
    this.task = task;
    this.timer = new Timer("Sovereign Finalizer", true);
    this.timerTask = new TimerTask() {
      @Override
      public void run() {
        try {
          reclaim();
        } catch (Throwable t) {
          LOGGER.warn("Error while reclaiming", t);
        }
      }
    };
    timer.schedule(timerTask, TimeUnit.MILLISECONDS.convert(period, unit),
      TimeUnit.MILLISECONDS.convert(period, unit));
  }

  private static void assertObjectEqualityOf(Class<?> type) {
    try {
      boolean hashCodeOverridden = !Object.class.equals(type.getMethod("hashCode").getDeclaringClass());
      boolean equalsOverridden = !Object.class.equals(type.getMethod("equals", Object.class).getDeclaringClass());
      if (hashCodeOverridden || equalsOverridden) {
        throw new AssertionError(type + " override java.lang.Object equality");
      }
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("unchecked")
  public void reclaim() {
    Wrapper<MARKER, OBJ> obj;
    while ((obj = (Wrapper<MARKER, OBJ>) refQueue.poll()) != null) {
      try {
        task.accept(obj.getObj());
      } finally {
        obj.reclaim();
      }
    }
  }

  public void record(MARKER marker, OBJ object) {
    map.put(object, new Wrapper<>(marker, object, map, refQueue));
  }

  public void remove(OBJ object) {
    map.remove(object);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  public void close() {
    try {
      timer.cancel();
    } finally {
      reclaim();
    }
  }
}
