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
package com.terracottatech.tools.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Adapted from https://stackoverflow.com/a/41406399
public class TableFormatter {
  private static final int PADDING_SIZE = 4;
  public static final String NEW_LINE = "\n";
  private static final char ROW_COLUMN_JOINER = '+';
  private static final char COLUMN_SEPARATOR = '|';
  private static final char ROW_COMPOSITION_CHAR = '-';

  public static String formatAsTable(List<String> headersList, List<List<String>> rowsList) {
    StringBuilder stringBuilder = new StringBuilder();
    Map<Integer, Integer> columnMaxWidthMapping = getMaxColumnWidths(headersList, rowsList);
    createHeader(headersList, stringBuilder, columnMaxWidthMapping);
    createBody(headersList, rowsList, stringBuilder, columnMaxWidthMapping);
    return stringBuilder.toString();
  }

  private static Map<Integer, Integer> getMaxColumnWidths(List<String> headersList, List<List<String>> rowsList) {
    Map<Integer, Integer> columnMaxWidthMapping = new HashMap<>();

    for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
      columnMaxWidthMapping.put(columnIndex, headersList.get(columnIndex).length());
    }

    for (List<String> row : rowsList) {
      int columnIndex = 0;
      for (; columnIndex < headersList.size(); columnIndex++) {
        if (row.get(columnIndex).length() > columnMaxWidthMapping.get(columnIndex)) {
          columnMaxWidthMapping.put(columnIndex, row.get(columnIndex).length());
        }
      }

      if (row.size() > columnIndex) {
        int maxColumnWidth = -1;
        for (int i = columnIndex; i < row.size(); i++) {
          if (row.get(i).length() > maxColumnWidth) {
            maxColumnWidth = row.get(i).length();
          }
        }

        if (maxColumnWidth > columnMaxWidthMapping.get(columnIndex - 1)) {
          columnMaxWidthMapping.put(columnIndex - 1, row.get(columnIndex).length());
        }
      }
    }

    for (int columnIndex = 0; columnIndex < headersList.size(); columnIndex++) {
      if (columnMaxWidthMapping.get(columnIndex) % 2 != 0) {
        columnMaxWidthMapping.put(columnIndex, columnMaxWidthMapping.get(columnIndex) + 1);
      }
    }
    return columnMaxWidthMapping;
  }

  private static void createHeader(List<String> headersList, StringBuilder stringBuilder, Map<Integer, Integer> columnMaxWidthMapping) {
    createBorder(stringBuilder, headersList.size(), columnMaxWidthMapping, ROW_COLUMN_JOINER, false);

    for (int headerIndex = 0; headerIndex < headersList.size(); headerIndex++) {
      fillCell(stringBuilder, headersList.get(headerIndex), headerIndex, columnMaxWidthMapping);
    }

    createBorder(stringBuilder, headersList.size(), columnMaxWidthMapping, ROW_COLUMN_JOINER, true);
  }

  private static void createBody(List<String> headersList, List<List<String>> rowsList, StringBuilder stringBuilder,
                                 Map<Integer, Integer> columnMaxWidthMapping) {
    for (int i = 0; i < rowsList.size(); i++) {
      List<String> row = rowsList.get(i);
      int cellIndex = 0;
      for (; cellIndex < headersList.size(); cellIndex++) {
        fillCell(stringBuilder, row.get(cellIndex), cellIndex, columnMaxWidthMapping);
      }

      for (; cellIndex < row.size(); cellIndex++) {
        stringBuilder.append(NEW_LINE);
        for (int j = 0; j < headersList.size(); j++) {
          if (j == headersList.size() - 1) {
            fillCell(stringBuilder, row.get(cellIndex), j, columnMaxWidthMapping);
          } else {
            fillCell(stringBuilder, "", j, columnMaxWidthMapping);
          }
        }
      }

      if (i != rowsList.size() - 1) {
        createBorder(stringBuilder, headersList.size(), columnMaxWidthMapping, ROW_COMPOSITION_CHAR, true);
      } else {
        stringBuilder.append(NEW_LINE);
      }
    }
    createBorder(stringBuilder, headersList.size(), columnMaxWidthMapping, ROW_COLUMN_JOINER, false);
  }

  private static void createBorder(StringBuilder stringBuilder, int headersListSize, Map<Integer, Integer> columnMaxWidthMapping,
                                   char cornerChar, boolean prependNewLine) {
    if (prependNewLine) {
      stringBuilder.append(NEW_LINE);
    }

    for (int i = 0; i < headersListSize; i++) {
      if (i == 0) {
        stringBuilder.append(cornerChar);
      }

      for (int j = 0; j < columnMaxWidthMapping.get(i) + PADDING_SIZE * 2; j++) {
        stringBuilder.append(ROW_COMPOSITION_CHAR);
      }
      stringBuilder.append(cornerChar);
    }
    stringBuilder.append(NEW_LINE);
  }

  private static void fillCell(StringBuilder stringBuilder, String cell, int cellIndex, Map<Integer, Integer> columnMaxWidthMapping) {
    int cellPaddingSize = getOptimumCellPadding(cellIndex, cell.length(), columnMaxWidthMapping);
    if (cellIndex == 0) {
      stringBuilder.append(COLUMN_SEPARATOR);
    }

    fillSpace(stringBuilder, cellPaddingSize);
    stringBuilder.append(cell);
    if (cell.length() % 2 != 0) {
      stringBuilder.append(" ");
    }

    fillSpace(stringBuilder, cellPaddingSize);
    stringBuilder.append(COLUMN_SEPARATOR);
  }

  private static int getOptimumCellPadding(int cellIndex, int dataLength, Map<Integer, Integer> columnMaxWidthMapping) {
    int cellPaddingSize = PADDING_SIZE;
    if (dataLength % 2 != 0) {
      dataLength++;
    }

    if (dataLength < columnMaxWidthMapping.get(cellIndex)) {
      cellPaddingSize = cellPaddingSize + (columnMaxWidthMapping.get(cellIndex) - dataLength) / 2;
    }
    return cellPaddingSize;
  }

  private static void fillSpace(StringBuilder stringBuilder, int length) {
    for (int i = 0; i < length; i++) {
      stringBuilder.append(" ");
    }
  }
}
