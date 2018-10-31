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

package com.terracottatech.ehcache.clustered.frs;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class FileUtils {
  public static void moveDataDirs(File rootDir, String...dataDirNames) throws IOException {
    Files.walkFileTree(rootDir.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (matchesDataDir(dir, dataDirNames)) {
          final Path backupPath = dir.getParent().resolve(dir.getFileName())
              .resolve(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss")));
          Files.createDirectories(backupPath);
          Files.move(dir, backupPath);
          return FileVisitResult.SKIP_SUBTREE;
        } else {
          return FileVisitResult.CONTINUE;
        }
      }
    });
  }

  public static void deleteDataDirs(File path, String...dataDirNames) throws IOException {
    Files.walkFileTree(path.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (matchesDataDir(dir, dataDirNames)) {
          recursivelyDeleteDirectories(dir);
          return FileVisitResult.SKIP_SUBTREE;
        } else {
          return FileVisitResult.CONTINUE;
        }
      }
    });
  }

  private static boolean matchesDataDir(Path dir, String...dataDirNames) {
    for (String dataDir : dataDirNames) {
      if (dir.endsWith(dataDir)) {
        return true;
      }
    }
    return false;
  }

  private static void recursivelyDeleteDirectories(Path rootPath) throws IOException {
    Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
}