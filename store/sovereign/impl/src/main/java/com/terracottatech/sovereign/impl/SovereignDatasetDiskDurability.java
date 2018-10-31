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
package com.terracottatech.sovereign.impl;

import com.terracottatech.store.configuration.BaseDiskDurability;
import com.terracottatech.store.configuration.DiskDurability;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SovereignDatasetDiskDurability implements Serializable {

  public static final SovereignDatasetDiskDurability NONE = new SovereignDatasetDiskDurability(DurabilityEnum.NONE);
  public static final SovereignDatasetDiskDurability ALWAYS = new SovereignDatasetDiskDurability(DurabilityEnum.ALWAYS);
  private static final long serialVersionUID = -1453974421938564251L;
  private final DurabilityEnum durabilityEnum;

  public enum DurabilityEnum {
    ALWAYS,
    TIMED,
    NONE
  }

  protected SovereignDatasetDiskDurability(DurabilityEnum anEnum) {
    durabilityEnum = anEnum;
  }

  public static SovereignDatasetDiskDurability timed(long dur, TimeUnit units) {
    return new Timed(dur, units);
  }

  public static SovereignDatasetDiskDurability convert(DiskDurability dur) {
    switch (dur.getDurabilityEnum()) {
      case EVENTUAL:
        return NONE;
      case EVERY_MUTATION:
        return ALWAYS;
      case TIMED:
        BaseDiskDurability.Timed timedDur = ((BaseDiskDurability.Timed) dur);
        // too small, screw it.
        if (timedDur.getMillisDuration() < 10) {
          return ALWAYS;
        }
        return new Timed(timedDur.getMillisDuration(), TimeUnit.MILLISECONDS);
      default:
        throw new IllegalStateException();
    }
  }

  public String toString() {
    return "SovereignDiskDurability: " + durabilityEnum.name();
  }

  public DurabilityEnum getDurabilityEnum() {
    return durabilityEnum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SovereignDatasetDiskDurability)) {
      return false;
    }
    SovereignDatasetDiskDurability that = (SovereignDatasetDiskDurability) o;
    return durabilityEnum == that.durabilityEnum;
  }

  @Override
  public int hashCode() {
    return Objects.hash(durabilityEnum);
  }

  @SuppressWarnings("serial")
  public static class Timed extends SovereignDatasetDiskDurability {
    private final long nsDuration;

    protected Timed(long duration, TimeUnit units) {
      super(DurabilityEnum.TIMED);
      if (duration <= 0) {
        throw new IllegalArgumentException();
      }
      this.nsDuration = TimeUnit.NANOSECONDS.convert(duration, units);
    }

    public long getNsDuration() {
      return nsDuration;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Timed)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Timed timed = (Timed) o;
      return nsDuration == timed.nsDuration;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), nsDuration);
    }

  }
}
