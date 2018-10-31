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
package com.terracottatech.sovereign.impl.dataset;

import com.terracottatech.sovereign.impl.compute.CellComparison;
import com.terracottatech.sovereign.plan.IndexedCellRange;
import com.terracottatech.sovereign.plan.IndexedCellSelection;
import com.terracottatech.sovereign.plan.Plan;
import com.terracottatech.sovereign.plan.SortedIndexPlan;
import com.terracottatech.sovereign.plan.StreamPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Tracks and builds the query plan statistics as the stream is processed by {@link RecordStreamImpl} and {@link SovereignOptimizer}
 * and various {@code Spliterators}.
 */
public final class StreamPlanTracker implements StreamPlan, StreamPlanWriter {

  /**
   * Create a faster singleton {@link StreamPlanWriter} and use this as the default query plan tracker.
   */
  private static final StreamPlanWriter DUMMY_STREAM_PLAN_WRITER = new StreamPlanWriter() {};

  static StreamPlanWriter getDummyWriter() {
    return DUMMY_STREAM_PLAN_WRITER;
  }

  private final List<String> unusedFilterList = new ArrayList<>();
  private final Collection<Consumer<StreamPlan>> consumers;

  private String usedFilterExpression = "true";
  private String normalizedFilterExpression;
  private int unknownFilterCount = 0;
  private Plan executingPlan = new DefaultPlan();
  private long planStartTime = 0L;
  private long planEndTime = 0L;

  StreamPlanTracker(Collection<Consumer<StreamPlan>> consumers) {
    this.consumers = consumers;
  }

  @Override
  public boolean isSortedCellIndexUsed() {
    switch (executingPlan.getPlanType()) {
      case SORTED_INDEX_SCAN:
      case SORTED_INDEX_MULTI_SCAN:
        return true;
      default:
        return false;
    }
  }

  @Override
  public String usedFilterExpression() {
    return usedFilterExpression;
  }

  @Override
  public Optional<String> getNormalizedFilterExpression() {
    return Optional.ofNullable(normalizedFilterExpression);
  }

  @Override
  public int unknownFilterCount() {
    return unknownFilterCount;
  }

  @Override
  public int unusedKnownFilterCount() {
    return unusedFilterList.size();
  }

  @Override
  public List<String> unusedKnownFilterExpression() {
    return Collections.unmodifiableList(unusedFilterList);
  }

  @Override
  public long planTimeInNanos() {
    return planEndTime - planStartTime;
  }

  @Override
  public Plan executingPlan() {
    return executingPlan;
  }

  @Override
  public void startPlanning() {
    planStartTime = System.nanoTime();
  }

  @Override
  public void endPlanning() {
    planEndTime = System.nanoTime();
    consumers.forEach(w -> w.accept(this));
  }

  @Override
  public void setResult(Optimization<?> result) {
    if (result.doSkipScan()) {
       executingPlan = new SkipScanPlan();
    } else {
      CellComparison<?> cellComparison = result.getCellComparison();
      if (cellComparison != null) {
        executingPlan = new DefaultSortedIndexPlan<>(cellComparison);
      }
    }
    normalizedFilterExpression = result.getNormalizedFilterExpression();
  }

  @Override
  public void incrementUnknownFilter() {
    unknownFilterCount++;
  }

  @Override
  public void setUsedFilterExpression(final String ufe) {
    usedFilterExpression = ufe;
  }

  @Override
  public void addUnusedFilterExpression(String unused) {
    unusedFilterList.add(unused);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Stream Planning Time (Nanoseconds): ").append(planTimeInNanos()).append("\n");
    sb.append("Sorted Index Used In Filter: ").append(isSortedCellIndexUsed()).append("\n");
    sb.append("Filter Expression: ").append(usedFilterExpression()).append("\n");
    getNormalizedFilterExpression().ifPresent((ne) -> sb.append("Normalized Filter Expression: ")
        .append(ne)
        .append("\n"));
    sb.append("Unknown Filter Count: ").append(unknownFilterCount()).append("\n");
    sb.append("Unused Filter Count And Filters (If Any): ").append(unusedKnownFilterCount()).append("\n");
    int i = 1;
    for (String s : unusedFilterList) {
      sb.append("   Unused Filter ").append(i++).append(": ").append(s).append("\n");
    }
    sb.append(executingPlan());
    return sb.toString();
  }

  /**
   * Default plan that represents a full dataset query.
   */
  private static final class DefaultPlan implements Plan {
    @Override
    public PlanType getPlanType() {
      return PlanType.FULL_DATASET_SCAN;
    }

    @Override
    public List<Plan> getSubPlans() {
      return null;
    }

    @Override
    public String toString() {
      return "Selected Plan: " + PlanType.FULL_DATASET_SCAN.getPlanName();
    }
  }

  /**
   * Default implementation of the {@code SortedIndexPlan} based on the {@link CellComparison} returned
   * by {@link SovereignOptimizer}.
   *
   * @param <K> Record key type
   * @param <V> Cell definition type
   */
  private static final class DefaultSortedIndexPlan<K extends Comparable<K>, V extends Comparable<V>>
      implements SortedIndexPlan<V> {
    private final IndexedCellSelection indexedCell;

    DefaultSortedIndexPlan(CellComparison<K> cellComparison) {
      this.indexedCell = cellComparison;
    }

    @Override
    public Plan.PlanType getPlanType() {
      return (indexedCell.indexRanges().size() > 1) ? PlanType.SORTED_INDEX_MULTI_SCAN :
          Plan.PlanType.SORTED_INDEX_SCAN;
    }

    @Override
    public List<Plan> getSubPlans() {
      // no sub plans for a simple sorted index plan
      return Collections.emptyList();
    }

    @Override
    public IndexedCellSelection indexedCellSelection() {
      return indexedCell;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Selected Plan: ").append(getPlanType().getPlanName()).append("\n");
      sb.append("  Index Ranges: ").append("(Number of Ranges = ").append(indexedCellSelection().countIndexRanges()).append(")\n");
      int i = 1;
      for (IndexedCellRange<?> ir : indexedCell.indexRanges()) {
        sb.append("     Cell Definition Name: ").append(ir.getCellDefinition().name()).append("\n");
        sb.append("         Cell Definition Type: ").append(ir.getCellDefinition().type().getJDKType().getSimpleName()).append("\n");
        sb.append("         Index Range ").append(i++).append(": Range = ");
        if (ir.end() != null) {
          sb.append(ir.start()).append(" to ").append(ir.end());
        } else {
          sb.append(ir.start());
        }
        sb.append(" ::: Operation = ").append(ir.operation().name()).append("\n");
      }
      return sb.toString();
    }
  }

  private static class SkipScanPlan implements Plan {
    @Override
    public PlanType getPlanType() {
      return PlanType.SKIP_SCAN;
    }

    @Override
    public List<Plan> getSubPlans() {
      return Collections.emptyList();
    }

    @Override
    public String toString() {
      return "Selected Plan: " + getPlanType().getPlanName() + "\n" +
              "Empty result set will be returned as the query is self contradictory.";
    }
  }
}
