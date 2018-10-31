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
package com.terracottatech.store.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * A representation of the asynchronous execution of an operation.  Asynchronous
 * dataset operations can be incorporated as stages of a larger computation
 * through use of the {@code CompletionStage} methods.
 *
 * @author Chris Dennis
 * @param <T> the type of the operation result
 */
public interface Operation<T> extends Future<T>, CompletionStage<T> {

  @Override
  public <U> Operation<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor);

  @Override
  public <U> Operation<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn);

  @Override
  public <U> Operation<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

  @Override
  public Operation<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor);

  @Override
  public Operation<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action);

  @Override
  public Operation<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

  @Override
  public Operation<T> exceptionally(Function<Throwable, ? extends T> fn);

  @Override
  public <U> Operation<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor);

  @Override
  public <U> Operation<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn);

  @Override
  public <U> Operation<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn);

  @Override
  public Operation<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor);

  @Override
  public Operation<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action);

  @Override
  public Operation<Void> runAfterEither(CompletionStage<?> other, Runnable action);

  @Override
  public Operation<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor);

  @Override
  public Operation<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action);

  @Override
  public Operation<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action);

  @Override
  public <U> Operation<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor);

  @Override
  public <U> Operation<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn);

  @Override
  public <U> Operation<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn);

  @Override
  public Operation<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor);

  @Override
  public Operation<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action);

  @Override
  public Operation<Void> runAfterBoth(CompletionStage<?> other, Runnable action);

  @Override
  public <U> Operation<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor);

  @Override
  public <U> Operation<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);

  @Override
  public <U> Operation<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);

  @Override
  public <U, V> Operation<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor);

  @Override
  public <U, V> Operation<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

  @Override
  public <U, V> Operation<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

  @Override
  public Operation<Void> thenRunAsync(Runnable action, Executor executor);

  @Override
  public Operation<Void> thenRunAsync(Runnable action);

  @Override
  public Operation<Void> thenRun(Runnable action);

  @Override
  public Operation<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor);

  @Override
  public Operation<Void> thenAcceptAsync(Consumer<? super T> action);

  @Override
  public Operation<Void> thenAccept(Consumer<? super T> action);

  @Override
  public <U> Operation<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor);

  @Override
  public <U> Operation<U> thenApplyAsync(Function<? super T, ? extends U> fn);

  @Override
  public <U> Operation<U> thenApply(Function<? super T, ? extends U> fn);

  /**
   * Returns a completed operation with the given result.
   *
   * @param <T> operation result type
   * @param result operation result
   * @return a completed operation
   */
  public static <T> Operation<T> completedOperation(final T result) {
    return operation(completedFuture(result));
  }

  /**
   * Returns the given CompletableFuture wrapped as an Operation.
   *
   * @param <T> the operation result type
   * @param future completable future to wrap
   * @return a {@code CompletableFuture} wrapped as an {@code Operation}
   */
  public static <T> Operation<T> operation(final CompletableFuture<T> future) {
    return new Operation<T>() {

      @Override
      public boolean isDone() {
        return future.isDone();
      }

      @Override
      public T get() throws InterruptedException, ExecutionException {
        return future.get();
      }

      @Override
      public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
      }

      @Override
      public <U> Operation<U> thenApply(Function<? super T, ? extends U> fn) {
        return operation(future.thenApply(fn));
      }

      @Override
      public <U> Operation<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return operation(future.thenApplyAsync(fn));
      }

      @Override
      public <U> Operation<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return operation(future.thenApplyAsync(fn, executor));
      }

      @Override
      public Operation<Void> thenAccept(Consumer<? super T> action) {
        return operation(future.thenAccept(action));
      }

      @Override
      public Operation<Void> thenAcceptAsync(Consumer<? super T> action) {
        return operation(future.thenAcceptAsync(action));
      }

      @Override
      public Operation<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return operation(future.thenAcceptAsync(action, executor));
      }

      @Override
      public Operation<Void> thenRun(Runnable action) {
        return operation(future.thenRun(action));
      }

      @Override
      public Operation<Void> thenRunAsync(Runnable action) {
        return operation(future.thenRunAsync(action));
      }

      @Override
      public Operation<Void> thenRunAsync(Runnable action, Executor executor) {
        return operation(future.thenRunAsync(action, executor));
      }

      @Override
      public <U, V> Operation<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return operation(future.thenCombine(other, fn));
      }

      @Override
      public <U, V> Operation<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return operation(future.thenCombineAsync(other, fn));
      }

      @Override
      public <U, V> Operation<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return operation(future.thenCombineAsync(other, fn, executor));
      }

      @Override
      public <U> Operation<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return operation(future.thenAcceptBoth(other, action));
      }

      @Override
      public <U> Operation<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return operation(future.thenAcceptBothAsync(other, action));
      }

      @Override
      public <U> Operation<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
        return operation(future.thenAcceptBothAsync(other, action, executor));
      }

      @Override
      public Operation<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return operation(future.runAfterBoth(other, action));
      }

      @Override
      public Operation<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return operation(future.runAfterBothAsync(other, action));
      }

      @Override
      public Operation<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return operation(future.runAfterBothAsync(other, action, executor));
      }

      @Override
      public <U> Operation<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return operation(future.applyToEither(other, fn));
      }

      @Override
      public <U> Operation<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return operation(future.applyToEitherAsync(other, fn));
      }

      @Override
      public <U> Operation<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return operation(future.applyToEitherAsync(other, fn, executor));
      }

      @Override
      public Operation<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return operation(future.acceptEither(other, action));
      }

      @Override
      public Operation<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return operation(future.acceptEitherAsync(other, action));
      }

      @Override
      public Operation<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return operation(future.acceptEitherAsync(other, action, executor));
      }

      @Override
      public Operation<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return operation(future.runAfterEither(other, action));
      }

      @Override
      public Operation<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return operation(future.runAfterEitherAsync(other, action));
      }

      @Override
      public Operation<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return operation(future.runAfterEitherAsync(other, action, executor));
      }

      @Override
      public <U> Operation<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return operation(future.thenCompose(fn));
      }

      @Override
      public <U> Operation<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return operation(future.thenComposeAsync(fn));
      }

      @Override
      public <U> Operation<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return operation(future.thenComposeAsync(fn, executor));
      }

      @Override
      public Operation<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return operation(future.whenComplete(action));
      }

      @Override
      public Operation<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return operation(future.whenCompleteAsync(action));
      }

      @Override
      public Operation<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return operation(future.whenCompleteAsync(action, executor));
      }

      @Override
      public <U> Operation<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return operation(future.handle(fn));
      }

      @Override
      public <U> Operation<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return operation(future.handleAsync(fn));
      }

      @Override
      public <U> Operation<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return operation(future.handleAsync(fn, executor));
      }

      @Override
      public CompletableFuture<T> toCompletableFuture() {
        return future;
      }

      @Override
      public Operation<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return operation(future.exceptionally(fn));
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return future.isCancelled();
      }
    };
  }
}
