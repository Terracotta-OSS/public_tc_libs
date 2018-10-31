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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

/**
 * File versioner. Manages a rolling set of files.
 *
 * @author cschanck
 */
public class FileVersioner {
  private final String prefix;
  private final File dir;

  /**
   * Instantiates a new File versioner.
   *
   * @param dir     the dir
   * @param pattern the prefix
   * @throws IOException the iO exception
   */
  public FileVersioner(File dir, String pattern) throws IOException {
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new IOException("Unable to makedir: " + dir);
      }
    }
    if (!dir.isDirectory()) {
      throw new IOException(dir + " is not a directory");
    }
    this.dir = dir;
    this.prefix = pattern;
  }

  /**
   * Gets latest existing file index.
   *
   * @return the latest existing file index
   */
  public int getLatestExistingFileIndex() {
    int largest = -1;
    File[] files = getAllVersionedFiles();
    for (File f : files) {
      String end = f.getName().substring(prefix.length());
      int ver = Integer.parseInt(end);
      if (ver > largest) {
        largest = ver;
      }
    }
    return largest;
  }

  /**
   * Gets latest existing file.
   *
   * @return the latest existing file
   */
  public File getLatestExistingFile() {
    int i = getLatestExistingFileIndex();
    if (i >= 0) {
      return makeVersionedFileName(i);
    }
    return null;
  }

  private File makeVersionedFileName(int i) {
    return new File(dir, prefix + i);
  }

  /**
   * Gets next versioned file.
   *
   * @return the next versioned file
   */
  public File getNextVersionedFile() {
    int latest = getLatestExistingFileIndex();
    return makeVersionedFileName(latest + 1);
  }

  public File[] getAllVersionedFiles() {
    if (dir.exists() && dir.isDirectory()) {
      return dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {

          if (name.startsWith(prefix)) {
            String numeric = name.substring(prefix.length());
            if (numeric.length() > 0) {
              try {
                Integer.parseInt(numeric);
                return true;
              } catch (Throwable t) {
              }
            }
          }
          return false;
        }
      });
    } else {
      return new File[0];
    }
  }

  public void deleteAll() throws IOException {
    for (File f : getAllVersionedFiles()) {
      FileUtils.deleteRecursively(f);
    }
  }

  /**
   * Gets prefix.
   *
   * @return the prefix
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Gets dir.
   *
   * @return the dir
   */
  public File getDir() {
    return dir;
  }
}
