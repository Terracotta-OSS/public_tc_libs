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
package com.terracottatech.sovereign.impl.persistence;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.terracottatech.sovereign.common.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Root containing a persistent store's file system.
 *
 * @author cschanck
 **/
public class PersistenceRoot implements Serializable {

  public enum Mode {
    /**
     * If it exists, delete it. Then create new.
     */
    CREATE_NEW,
    /**
     * Must be new. Will throw exception if it is present.
     */
    ONLY_IF_NEW,
    /**
     * Must exist. Will throw exception if it does not exist.
     */
    REOPEN,
    /**
     * Will reopen or create as needed.
     */
    DONTCARE
  }

  public static final String META = "meta";
  public static final String DATA = "data";
  public static final String PROPERTIES = "properties";
  private static final long serialVersionUID = 982502395675342583L;
  private final boolean reopen;
  private File root;
  private File metaRoot;
  private File dataRoot;
  private File propertiesFile;

  /**
   * Static helper. Recreate it even if exists.
   *
   * @param f File to root the persistence at.
   * @return new PersistenceRoot
   * @throws IOException if there is an io error
   */
  public static PersistenceRoot create(File f) throws IOException {
    return new PersistenceRoot(f, Mode.CREATE_NEW);
  }

  /**
   * Static helper. Only open if the designated root is empty.
   *
   * @param f File to root the persistence at.
   * @return new PersistenceRoot
   * @throws IOException if there is an io error
   */
  public static PersistenceRoot asNew(File f) throws IOException {
    return new PersistenceRoot(f, Mode.ONLY_IF_NEW);
  }

  /**
   * Static helper. Only open if the persistence root is already populated.
   *
   * @param f file to reopen
   * @return new PersistenceRoot
   * @throws IOException if there is an io error
   */
  public static PersistenceRoot reopen(File f) throws IOException {
    return new PersistenceRoot(f, Mode.REOPEN);
  }

  /**
   * Static helper. Open as long as the root seems proper. Reopen or create as needed.
   *
   * @param f file to open or create
   * @return new PersistenceRoot
   * @throws IOException if there is an io error
   */
  public static PersistenceRoot open(File f) throws IOException {
    return new PersistenceRoot(f, Mode.DONTCARE);
  }

  /**
   * return true if meta or data directory in the given path is not empty.
   *
   * @param dataRoot Path to check
   * @return dataroot
   */
  public static boolean containsMetaOrDataFiles(Path dataRoot) throws IOException {
    if (!Files.exists(dataRoot)) {
      return false;
    }
    Path metaRoot = dataRoot.resolve(META);
    Path data = dataRoot.resolve(DATA);
    try (Stream<Path> metaPath = Files.list(metaRoot) ;
         Stream<Path> dataPath = Files.list(data)) {
      return metaPath.findFirst().isPresent() || dataPath.findFirst().isPresent();
    }
  }

  /**
   * Create or reuse a new persistence root.
   *
   * @param f Root directory
   * @param openMode mode to open in.
   * @throws IOException when there is an IO Error
   */
  public PersistenceRoot(File f, Mode openMode) throws IOException {
    if (f == null) {
      throw new IllegalArgumentException();
    }
    this.root = f;
    metaRoot = new File(root, META);
    dataRoot = new File(root, DATA);
    propertiesFile = new File(root, PROPERTIES);
    switch (openMode) {
      case CREATE_NEW: {
        deleteAll();
        internalOpenAsNew();
        reopen = false;
        break;
      }
      case ONLY_IF_NEW: {
        internalOpenAsNew();
        reopen = false;
        break;
      }
      case REOPEN: {
        internalReopen();
        reopen = true;
        break;
      }
      case DONTCARE: {
        reopen = internalDontCare();
        break;
      }
      default:
        throw new IllegalStateException();
    }
  }

  protected boolean internalDontCare() throws IOException {
    if (metaExists()) {
      internalReopen();
      return true;
    } else {
      internalOpenAsNew();
      return false;
    }
  }

  protected void internalReopen() throws IOException {
    if (!metaExists()) {
      throw new FileNotFoundException(metaRoot.toString());
    }
    makeData();
  }

  protected void internalOpenAsNew() throws IOException {
    if (metaExists()) {
      throw new FileAlreadyExistsException(metaRoot.toString());
    }
    if (dataExists()) {
      throw new FileAlreadyExistsException(dataRoot.toString());
    }
    try {
      makeData();
      makeMeta();
    } catch (IOException e) {
      try {
        deleteMeta();
      } catch (Throwable t) {
        e.addSuppressed(t);
      }
      try {
        deleteData();
      } catch (Throwable t) {
        e.addSuppressed(t);
      }
      throw e;
    }
  }

  private boolean metaExists() {
    File[] contents = metaRoot.listFiles();
    return contents != null && contents.length > 0;
  }

  private boolean dataExists() {
    File[] contents = dataRoot.listFiles();
    return contents != null && contents.length > 0;
  }

  private void makeMeta() throws IOException {
    if (!metaExists()) {
      if (!metaRoot.exists()) {
        if (!metaRoot.mkdirs()) {
          throw new IOException("Unable to make meta directory: " + metaRoot);
        }
      }
    }
  }

  private void makeData() throws IOException {
    if (!dataExists()) {
      if (!dataRoot.exists()) {
        if (!dataRoot.mkdirs()) {
          throw new IOException("Unable to make data directory: " + dataRoot);
        }
      }
    }
  }

  private void deleteMeta() throws IOException {
    FileUtils.deleteRecursively(metaRoot);
  }

  private void deleteData() throws IOException {
    FileUtils.deleteRecursively(dataRoot);
  }

  private void deletePropertiesFile() throws IOException {
    if (propertiesFile.exists()) {
      if (!propertiesFile.delete()) {
        throw new IOException("Unable to delete " + propertiesFile.toString());
      }
    }
  }

  private void deleteAll() throws IOException {
    deleteData();
    deleteMeta();
    deletePropertiesFile();
  }

  public boolean isReopen() {
    return reopen;
  }

  public File getRoot() {
    return root;
  }

  public File getMetaRoot() {
    return metaRoot;
  }

  public File getDataRoot() {
    return dataRoot;
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")   // https://github.com/spotbugs/spotbugs/issues/493
  public Properties loadProperties() throws IOException {
    Properties props = new Properties();
    if (propertiesFile.exists()) {
      if (!propertiesFile.isFile()) {
        throw new IOException("Invalid Properties File : " + propertiesFile.toString());
      }
      try (FileInputStream in = new FileInputStream(propertiesFile)) {
        props.load(in);
      }
    }
    return props;
  }

  @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")   // https://github.com/spotbugs/spotbugs/issues/493
  public void storeProperties(Properties properties) throws IOException {
    try (FileOutputStream out = new FileOutputStream(propertiesFile)) {
      properties.store(out, "");
    }
  }

  public void cleanIfEmpty() {
    try {
      if (!containsMetaOrDataFiles(root.toPath())) {
        deleteAll();
      }
    } catch (IOException ignored) {
    }
  }

  @Override
  public String toString() {
    return "PersistenceRoot{" + "reopen=" + reopen + ", root=" + root + '}';
  }
}
