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
package com.terracottatech.sovereign.btrees.stores.location;

import com.terracottatech.sovereign.common.utils.FileUtils;
import com.terracottatech.sovereign.common.utils.FileVersioner;

import java.io.File;
import java.io.IOException;

/**
 * A store location that is rooted in a directory. Contains a versioner for the
 * endless generation of files.
 *
 * @author cschanck
 */
public class DirectoryStoreLocation implements StoreLocation {

  private static final String MARKER = "creation.mark";
  private static final String STORE_PATTERN = "store.v";
  private final File dir;
  private FileVersioner versioner;
  private File probe;

  /**
   * Instantiates a new Directory store location.
   *
   * @param dir the dir
   * @throws IOException the iO exception
   */
  public DirectoryStoreLocation(File dir) throws IOException {
    this.dir = dir;
    this.probe = new File(dir, MARKER);
  }

  @Override
  public boolean ensure() throws IOException {
    if ((!dir.exists()) && (!dir.mkdirs())) {
      throw new IOException("Unable to create: " + dir);
    }
    boolean ret = probe.createNewFile();
    versioner = new FileVersioner(dir, STORE_PATTERN);
    return ret;
  }

  @Override
  public boolean exists() throws IOException {
    return probe.exists();
  }

  @Override
  public boolean destroy() throws IOException {
    FileUtils.deleteRecursively(dir);
    return true;
  }

  /**
   * Gets versioner.
   *
   * @return the versioner
   */
  public FileVersioner getVersioner() {
    return versioner;
  }

  /**
   * Gets directory.
   *
   * @return the directory
   */
  public File getDirectory() {
    return dir;
  }

  public static DirectoryStoreLocation temp() throws IOException {
    File temp = File.createTempFile("dirstoreloc", "dir");
    if (!temp.delete()) {
      throw new IOException("Error deleting: " + temp);
    }
    return new DirectoryStoreLocation(temp);
  }

  public void reopen() throws IOException {
    if ((!dir.exists()) && (!dir.mkdirs())) {
      throw new IOException("Unable to create: " + dir);
    }
    if (!probe.exists()) {
      throw new IOException("Unable to create: " + dir);
    }
    versioner = new FileVersioner(dir, STORE_PATTERN);
  }
}
