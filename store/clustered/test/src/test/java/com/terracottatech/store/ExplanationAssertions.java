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

package com.terracottatech.store;

import com.terracottatech.store.client.stream.Explanation;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.Verifier;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertThat;

public class ExplanationAssertions extends Verifier {

  private final Set<AssertionError> unusedMatchers = new CopyOnWriteArraySet<>();

  @Override
  protected void verify() {
    unusedMatchers.stream().reduce((a, b) -> {
      a.addSuppressed(b);
      return a;
    }).ifPresent(e -> {
      throw e;
    });
  }

  public Consumer<Object> explanationMatches(Matcher<? super Explanation> matching) {
    AssertionError capturedStack = new AssertionError("Explanation not seen");
    unusedMatchers.add(capturedStack);
    return explanation -> {
      try {
        assertThat(explanation, new TypeSafeMatcher<Object>(Explanation.class) {
          @Override
          protected boolean matchesSafely(Object item) {
            return matching.matches(item);
          }

          @Override
          public void describeTo(Description description) {
            description.appendText("explanation with ").appendDescriptionOf(matching).appendText("]");
          }
        });
      } catch (AssertionError error) {
        error.setStackTrace(capturedStack.getStackTrace());
        throw error;
      } finally {
        unusedMatchers.remove(capturedStack);
      }
    };
  }

  public static Matcher<PipelineOperation> op(PipelineOperation.Operation type) {
    return new TypeSafeMatcher<PipelineOperation>() {

      @Override
      public void describeTo(Description description) {
        description.appendText("op[type=").appendValue(type).appendText("]");
      }

      @Override
      protected boolean matchesSafely(PipelineOperation item) {
        return item.getOperation().equals(type);
      }
    };
  }

  public static Matcher<PipelineOperation> opAndArgs(PipelineOperation.Operation type, String... arguments) {
    return new TypeSafeMatcher<PipelineOperation>() {

      @Override
      public void describeTo(Description description) {
        description.appendText("op[type=").appendValue(type)
                .appendValueList(", args=", ",", "]", arguments);
      }

      @Override
      protected boolean matchesSafely(PipelineOperation item) {
        return item.getOperation().equals(type) && checkArguments(item);
      }

      private boolean checkArguments(PipelineOperation item) {
        Object[] argStrings = item.getArguments()
                .stream()
                .map(String::valueOf)
                .toArray();
        return Arrays.equals(argStrings, arguments);
      }
    };
  }

  public static Matcher<Explanation> serverSidePipeline(Matcher<? super List<PipelineOperation>> matcher) {
    return new TypeSafeMatcher<Explanation>() {
      @Override
      protected boolean matchesSafely(Explanation item) {
        return matcher.matches(item.serverSidePipeline());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("server-side-pipeline=[").appendDescriptionOf(matcher).appendText("]");
      }
    };
  }

  public static Matcher<Explanation> clientSideStripePipeline(Matcher<? super List<PipelineOperation>> matcher) {
    return new TypeSafeMatcher<Explanation>() {
      @Override
      protected boolean matchesSafely(Explanation item) {
        return matcher.matches(item.clientSideStripePipeline());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("client-side-stripe-pipeline=[").appendDescriptionOf(matcher).appendText("]");
      }
    };
  }

  public static Matcher<Explanation> clientSideMergedPipeline(Matcher<? super List<PipelineOperation>> matcher) {
    return new TypeSafeMatcher<Explanation>() {
      @Override
      protected boolean matchesSafely(Explanation item) {
        return matcher.matches(item.clientSideMergedPipeline());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("client-side-merged-pipeline=[").appendDescriptionOf(matcher).appendText("]");
      }
    };
  }
}
