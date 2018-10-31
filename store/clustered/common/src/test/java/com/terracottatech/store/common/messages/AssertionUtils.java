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
package com.terracottatech.store.common.messages;

import com.terracottatech.store.Cell;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ludovic Orban
 */
public class AssertionUtils {

  public static void assertAllCellsEqual(Iterable<Cell<?>> cells1, Iterable<Cell<?>> cells2) {
    ArrayList<Cell<?>> cells1List = new ArrayList<>();
    cells1.forEach(cells1List::add);
    Cell<?>[] cells1Array = cells1List.toArray(new Cell<?>[0]);

    ArrayList<Cell<?>> cells2List = new ArrayList<>();
    cells2.forEach(cells2List::add);
    Cell<?>[] cells2Array = cells2List.toArray(new Cell<?>[0]);

    assertAllCellsEqual(cells1Array, cells2Array);
  }

  private static void assertAllCellsEqual(Cell<?>[] cells1Array, Cell<?>[] cells2Array) {
    assertThat(cells1Array.length, is(cells2Array.length));
    for (int i = 0; i < cells1Array.length; i++) {
      Cell<?> cell1 = cells1Array[i];
      Cell<?> cell2 = cells2Array[i];

      assertThat(cell1.definition().name(), is(cell2.definition().name()));
      assertThat(cell1.definition().type(), is(cell2.definition().type()));
      assertThat(cell1.value(), is(cell2.value()));
    }
  }

}
