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
package com.terracottatech.store.statistics;

import org.terracotta.statistics.StatisticType;

/**
 * All statistics calculated on datasets.
 */
public interface DatasetOutcomes {

  enum GetOutcome implements DatasetOutcomes {
    /** found the entry we were looking for */
    SUCCESS("Dataset:Get:Success"),
    /** hasn't found the entry */
    NOT_FOUND("Dataset:Get:NotFound"),
    /** something failed while retrieving */
    FAILURE("Dataset:Get:Failure");

    private final String statisticName;

    GetOutcome(String statisticName) {
      this.statisticName = statisticName;
    }

    @Override
    public StatisticType getStatisticType() {
      return StatisticType.COUNTER;
    }

    @Override
    public String getStatisticName() {
      return statisticName;
    }

    @Override
    public String toString() {
      return statisticName + "(" + getStatisticType() + ")";
    }
  }

  enum AddOutcome implements DatasetOutcomes {
    /** a new entry was created */
    SUCCESS("Dataset:Add:Success"),
    /** the entry wasn't created, such an entry already exists */
    ALREADY_EXISTS("Dataset:Add:AlreadyExists"),
    /** something failed during create */
    FAILURE("Dataset:Add:Failure");

    private final String statisticName;

    AddOutcome(String statisticName) {
      this.statisticName = statisticName;
    }

    @Override
    public StatisticType getStatisticType() {
      return StatisticType.COUNTER;
    }

    @Override
    public String getStatisticName() {
      return statisticName;
    }

    @Override
    public String toString() {
      return statisticName + "(" + getStatisticType() + ")";
    }
  }

  enum UpdateOutcome implements DatasetOutcomes {
    /** the entry was updated */
    SUCCESS("Dataset:Update:Success"),
    /** no entry to update */
    NOT_FOUND("Dataset:Update:NotFound"),
    /** something failed during update */
    FAILURE("Dataset:Update:Failure");

    private final String statisticName;

    UpdateOutcome(String statisticName) {
      this.statisticName = statisticName;
    }

    @Override
    public StatisticType getStatisticType() {
      return StatisticType.COUNTER;
    }

    @Override
    public String getStatisticName() {
      return statisticName;
    }

    @Override
    public String toString() {
      return statisticName + "(" + getStatisticType() + ")";
    }
  }

  enum DeleteOutcome implements DatasetOutcomes {
    /** the entry was deleted */
    SUCCESS("Dataset:Delete:Success"),
    /** no entry to delete */
    NOT_FOUND("Dataset:Delete:NotFound"),
    /** something failed during deletion */
    FAILURE("Dataset:Delete:Failure");

    private final String statisticName;

    DeleteOutcome(String statisticName) {
      this.statisticName = statisticName;
    }

    @Override
    public StatisticType getStatisticType() {
      return StatisticType.COUNTER;
    }

    @Override
    public String getStatisticName() {
      return statisticName;
    }

    @Override
    public String toString() {
      return statisticName + "(" + getStatisticType() + ")";
    }
  }

  enum StreamOutcome implements DatasetOutcomes {
    /** a stream of records was requested */
    SUCCESS("Dataset:Stream:Request"),
    /** something failed when requesting records */
    FAILURE("Dataset:Stream:Failure");

    private final String statisticName;

    StreamOutcome(String statisticName) {
      this.statisticName = statisticName;
    }

    @Override
    public StatisticType getStatisticType() {
      return StatisticType.COUNTER;
    }

    @Override
    public String getStatisticName() {
      return statisticName;
    }

    @Override
    public String toString() {
      return statisticName + "(" + getStatisticType() + ")";
    }
  }

  StatisticType getStatisticType();

  String getStatisticName();

}
