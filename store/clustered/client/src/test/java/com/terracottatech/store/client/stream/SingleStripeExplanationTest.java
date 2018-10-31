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

package com.terracottatech.store.client.stream;

import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import org.junit.Test;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static com.terracottatech.store.common.dataset.stream.PipelineOperation.IntermediateOperation.FILTER;
import static com.terracottatech.store.common.dataset.stream.PipelineOperation.TerminalOperation.COUNT;
import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SingleStripeExplanationTest {

  @Test
  public void testClientSideMergedPipelineIsAlwaysEmpty() {
    SingleStripeExplanation explanation = new SingleStripeExplanation(
        singletonList(COUNT.newInstance()),
        emptyList(),
        emptyList());

    assertThat(explanation.clientSideMergedPipeline(), empty());
  }

  @Test
  public void testClientSideStripePipelineContents() {
    PipelineOperation op1 = FILTER.newInstance((Predicate<Object>) x -> true);
    PipelineOperation op2 = COUNT.newInstance();
    SingleStripeExplanation explanation = new SingleStripeExplanation(
        asList(op1, op2),
        emptyList(),
        emptyList());

    assertThat(explanation.clientSideStripePipeline(), contains(op1, op2));
  }

  @Test
  public void testServerSidePipelineContents() {
    PipelineOperation op1 = FILTER.newInstance((Predicate<Object>) x -> true);
    PipelineOperation op2 = COUNT.newInstance();
    SingleStripeExplanation explanation = new SingleStripeExplanation(
        emptyList(),
        asList(op1, op2),
        emptyList());

    assertThat(explanation.serverSidePipeline(), contains(op1, op2));
  }

  @Test
  public void testAbsentServerPlan() {
    SingleStripeExplanation explanation = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        emptyList());

    assertThat(explanation.serverPlans().keySet(), empty()) ;
  }

  @Test
  public void testServerPlanContents() {
    UUID uuid = UUID.randomUUID();
    String plan = "A plan so cunning you could put a tail on it and call it a weasel.";

    Map.Entry<UUID, String> serverPlan = new SimpleImmutableEntry<>(uuid, plan);
    SingleStripeExplanation explanation = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        singletonList(serverPlan));

    assertThat(explanation.serverPlans(), hasEntry(uuid, plan));
  }

  @Test
  public void testMultipleServerPlanContents() {
    UUID uuid1 = UUID.randomUUID();
    String plan1 = "A plan so cunning you could put a tail on it and call it a weasel.";
    Map.Entry<UUID, String> serverPlan1 = new SimpleImmutableEntry<>(uuid1, plan1);
    UUID uuid2 = UUID.randomUUID();
    String plan2 = "A plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.";
    Map.Entry<UUID, String> serverPlan2 = new SimpleImmutableEntry<>(uuid2, plan2);

    SingleStripeExplanation explanation = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        asList(serverPlan1, serverPlan2));

    assertThat(explanation.serverPlans(), hasEntry(uuid2, plan2));
    assertThat(explanation.failedServerPlans(), hasEntry(uuid1, plan1));
  }

  @Test
  public void testToStringFormatting() {
    UUID uuid1 = UUID.fromString("36fdedad-8402-4106-8fc6-e8503b790994");
    String plan1 = "A plan so cunning you could put a tail on it and call it a weasel.";
    Map.Entry<UUID, String> serverPlan1 = new SimpleImmutableEntry<>(uuid1, plan1);
    UUID uuid2 = UUID.fromString("d86e3447-6a08-4f41-b33a-f5617b418ea9");
    String plan2 = "A plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.";
    Map.Entry<UUID, String> serverPlan2 = new SimpleImmutableEntry<>(uuid2, plan2);

    PipelineOperation op1 = FILTER.newInstance(defineBool("wibble").isTrue());
    PipelineOperation op2 = COUNT.newInstance();

    SingleStripeExplanation explanation = new SingleStripeExplanation(
        asList(op1, op2),
        asList(op2, op1),
        asList(serverPlan1, serverPlan2));

    assertThat(explanation.toString(), is(
        "Stream Plan\n"
            + "\tStructure:\n"
            + "\t\tPortable:\n"
            + "\t\t\tPipelineOperation{COUNT()}\n"
            + "\t\t\tPipelineOperation{FILTER((wibble==true))}\n"
            + "\t\tNon-Portable:\n"
            + "\t\t\tPipelineOperation{FILTER((wibble==true))}\n"
            + "\t\t\tPipelineOperation{COUNT()}\n"
            + "\tServer Plan: [stream id: d86e3447-6a08-4f41-b33a-f5617b418ea9]\n"
            + "\t\tA plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.\n"
            + "\tFailed Server Plans:\n"
            + "\t\tServer Plan: [stream id: 36fdedad-8402-4106-8fc6-e8503b790994]\n"
            + "\t\t\tA plan so cunning you could put a tail on it and call it a weasel."));

  }
}
