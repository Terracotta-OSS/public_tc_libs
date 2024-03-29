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
package com.terracottatech.store.server;

import com.terracottatech.store.DatasetReader;
import com.terracottatech.store.DatasetWriterReader;
import com.terracottatech.store.common.dataset.stream.PipelineOperation;
import com.terracottatech.store.stream.MutableRecordStream;
import com.terracottatech.store.stream.RecordStream;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;

import static com.terracottatech.store.ExplanationAssertions.op;

enum StreamStrategy {
  DEFAULT,
  PARALLEL {
    @Override
    <K extends Comparable<K>> MutableRecordStream<K> stream(DatasetWriterReader<K> access) {
      return super.stream(access).parallel();
    }

    @Override
    <K extends Comparable<K>> RecordStream<K> stream(DatasetReader<K> access) {
      return super.stream(access).batch(1).parallel();
    }

    Matcher<? super List<PipelineOperation>> ignoringPrefix(Matcher<? super List<PipelineOperation>> matcher) {
      return new TypeSafeMatcher<List<PipelineOperation>>() {
        @Override
        protected boolean matchesSafely(List<PipelineOperation> item) {
          if (item.isEmpty()) {
            return matcher.matches(item);
          }
          if (op(PipelineOperation.IntermediateOperation.PARALLEL).matches(item.get(0))) {
            return matcher.matches(item.subList(1, item.size()));
          } else {
            return matcher.matches(item);
          }
        }

        @Override
        public void describeTo(Description description) {
          description.appendText("ignoring parallel() operation ").appendDescriptionOf(matcher);
        }
      };
    }
  },
  BATCHED_1 {
    @Override
    <K extends Comparable<K>> MutableRecordStream<K> stream(DatasetWriterReader<K> access) {
      return super.stream(access).batch(1);
    }

    @Override
    <K extends Comparable<K>> RecordStream<K> stream(DatasetReader<K> access) {
      return super.stream(access).batch(1);
    }
  },
  BATCHED_2 {
    @Override
    <K extends Comparable<K>> MutableRecordStream<K> stream(DatasetWriterReader<K> access) {
      return super.stream(access).batch(2);
    }

    @Override
    <K extends Comparable<K>> RecordStream<K> stream(DatasetReader<K> access) {
      return super.stream(access).batch(2);
    }
  },
  INLINE {
    @Override
    <K extends Comparable<K>> MutableRecordStream<K> stream(DatasetWriterReader<K> access) {
      return super.stream(access).inline();
    }

    @Override
    <K extends Comparable<K>> RecordStream<K> stream(DatasetReader<K> access) {
      return super.stream(access).inline();
    }
  };

  <K extends Comparable<K>> MutableRecordStream<K> stream(DatasetWriterReader<K> access) {
    return access.records();
  }

  <K extends Comparable<K>> RecordStream<K> stream(DatasetReader<K> access) {
    return access.records();
  }

  Matcher<? super List<PipelineOperation>> ignoringPrefix(Matcher<? super List<PipelineOperation>> matcher) {
    return matcher;
  }
}
