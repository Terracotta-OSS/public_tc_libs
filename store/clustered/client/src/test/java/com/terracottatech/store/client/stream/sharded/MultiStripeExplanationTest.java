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

package com.terracottatech.store.client.stream.sharded;

import com.terracottatech.store.client.stream.SingleStripeExplanation;
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
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MultiStripeExplanationTest {

  @Test
  public void testClientSideMergedPipelineContents() {
    PipelineOperation op1 = FILTER.newInstance((Predicate<Object>) x -> true);
    PipelineOperation op2 = COUNT.newInstance();
    MultiStripeExplanation explanation = new MultiStripeExplanation();
    explanation.addMergedPipeline(asList(op1, op2));

    assertThat(explanation.clientSideMergedPipeline(), contains(op1, op2));
  }

  @Test
  public void testClientSideStripePipelineReturnsAnyStripe() {
    PipelineOperation op1 = FILTER.newInstance((Predicate<Object>) x -> true);
    PipelineOperation op2 = COUNT.newInstance();
    SingleStripeExplanation stripeExplanation = new SingleStripeExplanation(
        asList(op1, op2),
        emptyList(),
        emptyList());

    MultiStripeExplanation explanation = new MultiStripeExplanation();
    explanation.addStripeExplanation(stripeExplanation);

    assertThat(explanation.clientSideStripePipeline(), contains(op1, op2));
  }

  @Test
  public void testServerSidePipelineReturnsAnyStripe() {
    PipelineOperation op1 = FILTER.newInstance((Predicate<Object>) x -> true);
    PipelineOperation op2 = COUNT.newInstance();
    SingleStripeExplanation stripeExplanation = new SingleStripeExplanation(
        emptyList(),
        asList(op1, op2),
        emptyList());

    MultiStripeExplanation explanation = new MultiStripeExplanation();
    explanation.addStripeExplanation(stripeExplanation);

    assertThat(explanation.serverSidePipeline(), contains(op1, op2));
  }

  @Test
  public void testServerPlansAggregatesStripes() {
    UUID uuidA = UUID.fromString("ba1d21c6-6a08-4f41-b33a-f5617b418ea9");
    String planA = "A cunning plan.";
    Map.Entry<UUID, String> serverPlanA = new SimpleImmutableEntry<>(uuidA, planA);
    SingleStripeExplanation stripeExplanationA = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        asList(serverPlanA));

    UUID uuidB = UUID.fromString("ba1d21c6-8402-4106-8fc6-e8503b790995");
    String planB = "A plan so cunning you could put a tail on it and call it a weasel.";
    Map.Entry<UUID, String> serverPlanB = new SimpleImmutableEntry<>(uuidB, planB);
    SingleStripeExplanation stripeExplanationB = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        asList(serverPlanB));

    MultiStripeExplanation explanation = new MultiStripeExplanation();
    explanation.addStripeExplanation(stripeExplanationA);
    explanation.addStripeExplanation(stripeExplanationB);

    assertThat(explanation.serverPlans(), hasEntry(uuidA, planA));
    assertThat(explanation.serverPlans(), hasEntry(uuidB, planB));
  }


  @Test
  public void testFailedServerPlansAggregatesStripes() {
    UUID uuidA1 = UUID.fromString("ba1d21c6-8402-4106-8fc6-e8503b790994");
    String planA1 = "A cunning plan.";
    Map.Entry<UUID, String> serverPlanA1 = new SimpleImmutableEntry<>(uuidA1, planA1);
    UUID uuidA2 = UUID.fromString("ba1d21c6-6a08-4f41-b33a-f5617b418ea9");
    String planA2 = "A cunning plan that cannot fail!";
    Map.Entry<UUID, String> serverPlanA2 = new SimpleImmutableEntry<>(uuidA2, planA2);

    UUID uuidB1 = UUID.fromString("ba1d21c6-8402-4106-8fc6-e8503b790995");
    String planB1 = "A plan so cunning you could put a tail on it and call it a weasel.";
    Map.Entry<UUID, String> serverPlanB1 = new SimpleImmutableEntry<>(uuidB1, planB1);
    UUID uuidB2 = UUID.fromString("ba1d21c6-6a08-4f41-b33a-f5617b418eaa");
    String planB2 = "A plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.";
    Map.Entry<UUID, String> serverPlanB2 = new SimpleImmutableEntry<>(uuidB2, planB2);

    SingleStripeExplanation stripeExplanationA = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        asList(serverPlanA1, serverPlanA2));

    SingleStripeExplanation stripeExplanationB = new SingleStripeExplanation(
        emptyList(),
        emptyList(),
        asList(serverPlanB1, serverPlanB2));


    MultiStripeExplanation explanation = new MultiStripeExplanation();
    explanation.addStripeExplanation(stripeExplanationA);
    explanation.addStripeExplanation(stripeExplanationB);

    assertThat(explanation.failedServerPlans(), hasEntry(uuidA1, planA1));
    assertThat(explanation.failedServerPlans(), hasEntry(uuidB1, planB1));
  }

  @Test
  public void testToStringFormatting() {
    UUID uuidA1 = UUID.fromString("ba1d21c6-8402-4106-8fc6-e8503b790994");
    String planA1 = "A cunning plan.";
    Map.Entry<UUID, String> serverPlanA1 = new SimpleImmutableEntry<>(uuidA1, planA1);
    UUID uuidA2 = UUID.fromString("ba1d21c6-6a08-4f41-b33a-f5617b418ea9");
    String planA2 = "A cunning plan that cannot fail!";
    Map.Entry<UUID, String> serverPlanA2 = new SimpleImmutableEntry<>(uuidA2, planA2);

    UUID uuidB1 = UUID.fromString("ba1d21c6-8402-4106-8fc6-e8503b790995");
    String planB1 = "A plan so cunning you could put a tail on it and call it a weasel.";
    Map.Entry<UUID, String> serverPlanB1 = new SimpleImmutableEntry<>(uuidB1, planB1);
    UUID uuidB2 = UUID.fromString("ba1d21c6-6a08-4f41-b33a-f5617b418eaa");
    String planB2 = "A plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.";
    Map.Entry<UUID, String> serverPlanB2 = new SimpleImmutableEntry<>(uuidB2, planB2);

    PipelineOperation op1 = FILTER.newInstance(defineBool("wibble").isTrue());
    PipelineOperation op2 = COUNT.newInstance();

    SingleStripeExplanation explanationA = new SingleStripeExplanation(
        asList(op2),
        asList(op1),
        asList(serverPlanA1, serverPlanA2));

    SingleStripeExplanation explanationB = new SingleStripeExplanation(
        asList(op2),
        asList(op1),
        asList(serverPlanB1, serverPlanB2));

    MultiStripeExplanation explanation = new MultiStripeExplanation();

    explanation.addStripeExplanation(explanationA);
    explanation.addStripeExplanation(explanationB);

    explanation.addMergedPipeline(asList(op2));

    assertThat(explanation.toString(), is("Stream Plan\n" +
        "\tStructure:\n" +
        "\t\tPortable:\n" +
        "\t\t\tPipelineOperation{FILTER((wibble==true))}\n" +
        "\t\tNon-Portable:\n" +
        "\t\t\tParallel:\n" +
        "\t\t\t\tPipelineOperation{COUNT()}\n" +
        "\t\t\tCommon (Merged):\n" +
        "\t\t\t\tPipelineOperation{COUNT()}\n" +
        "\tServer Plans:\n" +
        "\t\tServer Plan: [stream id: ba1d21c6-6a08-4f41-b33a-f5617b418ea9]\n" +
        "\t\t\tA cunning plan that cannot fail!\n" +
        "\t\tServer Plan: [stream id: ba1d21c6-6a08-4f41-b33a-f5617b418eaa]\n" +
        "\t\t\tA plan as cunning as a fox who's just been appointed Professor of Cunning at Oxford University.\n" +
        "\tFailed Server Plans:\n" +
        "\t\tServer Plan: [stream id: ba1d21c6-8402-4106-8fc6-e8503b790994]\n" +
        "\t\t\tA cunning plan.\n" +
        "\t\tServer Plan: [stream id: ba1d21c6-8402-4106-8fc6-e8503b790995]\n" +
        "\t\t\tA plan so cunning you could put a tail on it and call it a weasel."));

  }
}
