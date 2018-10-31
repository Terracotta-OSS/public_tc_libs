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
package com.terracottatech.store.common.dataset.stream;

import com.terracottatech.store.UpdateOperation;
import com.terracottatech.store.intrinsics.impl.Constant;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * Describes a {@code Stream} operation in a pipeline.
 *
 * @author Clifford W. Johnson
 */
public final class PipelineOperation {

  /**
   * Identifies the type of {@code Stream} operation.
   */
  private final Operation operation;

  /**
   * An opaque, application-provided descriptor for this {@code PipelineOperation}.
   * Extensions to a <i>wrapped stream</i> infrastructure may use this field to
   * provide additional information for pipeline operation analysis.
   */
  private final OperationMetaData operationMetaData;

  /**
   * A reference to the operation arguments relevant to post-processing the pipeline.
   */
  private final List<Object> arguments;

  /**
   * Constructs an {@code OperationDescriptor} for an operation in a stream pipeline.
   *
   * @param operation the {@code Stream} {@code Operation}; must not be {@code null}
   * @param operationMetaData an application-provided object to associate with this {@code PipelineOperation};
   *                          may be {@code null}
   * @param arguments the {@code operation} arguments relevant to pipeline post-processing;
   *                  must not be {@code null} but may be empty
   */
  private PipelineOperation(final Operation operation, final OperationMetaData operationMetaData, final List<Object> arguments) {
    this.operation = Objects.requireNonNull(operation, "operation");
    this.arguments = Objects.requireNonNull(arguments, "arguments");
    this.operationMetaData = operationMetaData;
  }

  /**
   * Gets the operation identification.
   *
   * @return the type of {@code Stream} operation
   */
  public Operation getOperation() {
    return this.operation;
  }

  /**
   * Gets the argument references for this operation considered relevant to {@code Stream}
   * post-processing.
   *
   * @return the operation arguments considered relevant to {@code Stream} post-processing
   */
  public List<Object> getArguments() {
    return this.arguments;
  }

  /**
   * Gets the application-provided object associated with this {@code PipelineOperation}.
   *
   * @return the application-provided object; may be {@code null}
   */
  public OperationMetaData getOperationMetaData() {
    return this.operationMetaData;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PipelineOperation{");
    sb.append(operation);
    sb.append(arguments.stream().map(Objects::toString).collect(joining(",", "(", ")")));
    if (operationMetaData != null) {
      sb.append(", operationMetaData=").append(operationMetaData);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Flags application-provided meta data related to a {@link PipelineOperation} instance.
   *
   * @see PipelineOperation#getOperationMetaData()
   */
  public interface OperationMetaData {
  }

  /**
   * Identifies a {@code Stream} operation.
   */
  public interface Operation {

    /**
     * Gets the name of this {@code operation}.
     *
     * @return the operation name
     */
    String name();

    /**
     * Indicates if this {@code Operation} is a terminal stream operation.
     *
     * @return {@code true} if this {@code Operation} represents a terminal operation;
     *    {@code false} if the operation is an intermediate operation.
     */
    boolean isTerminal();

    /**
     * Indicates if this {@code Operation} is a <i>short-circuiting</i> operation and
     * may cause early termination of a {@code Stream}.
     *
     * @return {@code true} if this {@code Operation} is a short-circuiting operation
     */
    @SuppressWarnings("unused")
    boolean isShortCircuit();

    /**
     * Gets the expected types of the arguments for this {@code Operation}.
     *
     * @return a non-null {@code List} of the classes of the operation's expected arguments
     */
    List<Class<?>> getArgumentTypes();

    /**
     * Create a {@link PipelineOperation} instance for this {@code operation} with
     * the arguments provided.
     * <p>The default implementation call is the same as calling {@code newInstance(null, arguments)}.
     *
     * @param arguments the operation arguments to preserve
     *
     * @return a new {@code PipelineOperation}
     */
    default PipelineOperation newInstance(Object... arguments) {
      return this.newInstance(null, arguments);
    }

    /**
     * Create a {@link PipelineOperation} instance for this {@code operation} with
     * the arguments provided.
     * <p>The default implementation ensures the arguments are the same number as
     * and assignment-compatible with {@link #getArgumentTypes()}.
     *
     * @param operationMetaData an application-provided object to associate with this {@code PipelineOperation};
     *                          may be {@code null}
     * @param arguments the operation arguments to preserve
     *
     * @return a new {@code PipelineOperation}
     */
    default PipelineOperation newInstance(final OperationMetaData operationMetaData, final Object... arguments) {
      final List<Class<?>> argumentTypes = this.getArgumentTypes();
      final List<Object> argumentList = Arrays.asList(arguments);

      /*
       * Verify that the captured arguments are assignment-compatible with the expected argument types.
       */
      if (!compare(argumentTypes, argumentList,
          (type, argument) -> argument == null || type.isInstance(argument)
              || ((argument instanceof Constant) && type.isInstance(((Constant)argument).getValue())))) {
        throw new IllegalStateException(
            String.format("%s operation argument mismatch; expecting [%s], found [%s]", this.name(),
                argumentTypes.stream().map(Class::getName).collect(joining(", ")),
                argumentList.stream().map(o -> (o == null ? "null" : o.getClass().getName())).collect(joining(", "))));
      }

      return new PipelineOperation(this, operationMetaData, Arrays.asList(arguments));
    }

    /**
     * Appends a reconstruction of this {@code Operation} to the stream provided.
     * @param stream the {@code BaseStream} subtype instance to which this {@code Operation} is appended
     * @param arguments the non-{@code null} list of arguments for to use; the specific argument requirements are
     *                  operation-specific
     * @return the result of appending this {@code Operation} to {@code stream}; the specific return type
     *        is operation-determined
     *
     * @throws ClassCastException if {@code stream} is not of the expected type or an {@code arguments} item
     *        is not of the expected type
     * @throws UnsupportedOperationException if this {@code Operation} does not support reconstruction
     */
    @SuppressWarnings("unused")
    default Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
      throw new UnsupportedOperationException("Reconstruction for " + this + " not supported");
    }

    /**
     * Indicates if this {@code Operation} supports reconstruction via the {@link #reconstruct(BaseStream, List)}
     * methods.
     * @return {@code true} if reconstruction is supported; {@code false} otherwise
     */
    default boolean supportsReconstruction() {
      try {
        this.getClass().getDeclaredMethod("reconstruct", BaseStream.class, List.class);
        return true;
      } catch (NoSuchMethodException e) {
        return false;
      }
    }
  }

  /**
   * Compares elements, in order, of two {@code List} instances with potentially different element types using a
   * {@code BiFunction} as an equals function.
   *
   * @param first the first list of elements to compare
   * @param second the second list of element to compare
   * @param comparator the {@code BiFunction} used to compare the elements
   * @param <T> the element type of {@code first}
   * @param <U> the element type of {@code second}
   *
   * @return {@code false} if {@code first} and {@code second} are not the same length or
   *      <code>comparator.apply(first.get(<i>i</i>), second.get(<i>i</i>))</code> returns {@code false}
   *      for any <i>i</i>; {@code true} if the lists are the same length and {@code comparator.apply}
   *      returns {@code true} for all element pairs
   */
  private static <T, U> boolean compare(final List<T> first, final List<U> second, final BiFunction<T, U, Boolean> comparator) {
    if (first.size() == second.size()) {
      for (int i = 0; i < first.size(); i++) {
        if (!comparator.apply(first.get(i), second.get(i))) {
          return false;
        }
      }
      return true;
    } else {
      return  false;
    }
  }

  /**
   * Identifies an <i>intermediate</i> {@code Stream} operation.
   *
   * See the <a href="doc-files/streamOperations.html#intermediate">intermediate operations summary</a> for a
   * list of the intermediate operations showing their operand types and the Java stream implementation
   * classes with which they're used.
   */
  @SuppressWarnings("unused")
  public enum IntermediateOperation implements Operation {
    AS_DOUBLE_STREAM(false, true) {
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof IntStream) {
          return ((IntStream)stream).asDoubleStream();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).asDoubleStream();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    AS_LONG_STREAM(false, true) {
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof IntStream) {
          return ((IntStream)stream).asLongStream();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    BOXED(false, true) {
      @Override
      public  Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).boxed();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).boxed();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).boxed();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    /**
     * TC Store {@code Record} delete intermediate operation.
     */
    DELETE_THEN(false, true) {
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        // Reconstruction of this operation must be handled specially
        throw new AssertionError("Reconstruction for " + this
            + " not supported against " + stream.getClass().getSimpleName());
      }
    },
    DISTINCT(false, true) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).distinct();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).distinct();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).distinct();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).distinct();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    DOUBLE_FILTER(false, true, DoublePredicate.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).filter((DoublePredicate)arguments.get(0));   // unchecked
      }
    },
    DOUBLE_FLAT_MAP(false, true, DoubleFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).flatMap((DoubleFunction) arguments.get(0));   // unchecked
      }
    },
    DOUBLE_MAP(false, true, DoubleUnaryOperator.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).map((DoubleUnaryOperator) arguments.get(0));   // unchecked
      }
    },
    DOUBLE_MAP_TO_INT(false, true, DoubleToIntFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).mapToInt((DoubleToIntFunction) arguments.get(0));   // unchecked
      }
    },
    DOUBLE_MAP_TO_LONG(false, true, DoubleToLongFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).mapToLong((DoubleToLongFunction) arguments.get(0));   // unchecked
      }
    },
    DOUBLE_MAP_TO_OBJ(false, true, DoubleFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((DoubleStream)stream).mapToObj((DoubleFunction) arguments.get(0));   // unchecked
      }
    },
    DOUBLE_PEEK(DoubleConsumer.class),
    FILTER(false, true, Predicate.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).filter((Predicate)arguments.get(0));   // unchecked
      }
    },
    FLAT_MAP(false, true, Function.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).flatMap((Function) arguments.get(0));   // unchecked
      }
    },
    FLAT_MAP_TO_DOUBLE(false, true, Function.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).flatMapToDouble((Function) arguments.get(0));   // unchecked
      }
    },
    FLAT_MAP_TO_INT(false, true, Function.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).flatMapToInt((Function) arguments.get(0));   // unchecked
      }
    },
    FLAT_MAP_TO_LONG(false, true, Function.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).flatMapToLong((Function) arguments.get(0));   // unchecked
      }
    },
    INT_FILTER(false, true, IntPredicate.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).filter((IntPredicate)arguments.get(0));   // unchecked
      }
    },
    INT_FLAT_MAP(false, true, IntFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).flatMap((IntFunction)arguments.get(0));   // unchecked
      }
    },
    INT_MAP(false, true, IntUnaryOperator.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).map((IntUnaryOperator)arguments.get(0));   // unchecked
      }
    },
    INT_MAP_TO_DOUBLE(false, true, IntToDoubleFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).mapToDouble((IntToDoubleFunction)arguments.get(0));   // unchecked
      }
    },
    INT_MAP_TO_LONG(false, true, IntToLongFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).mapToLong((IntToLongFunction) arguments.get(0));   // unchecked
      }
    },
    INT_MAP_TO_OBJ(false, true, IntFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((IntStream)stream).mapToObj((IntFunction) arguments.get(0));   // unchecked
      }
    },
    INT_PEEK(IntConsumer.class),
    LIMIT(true, true, Long.class) {
      @SuppressWarnings("unchecked")
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Long maxSize = ((Constant<?, Long>)arguments.get(0)).getValue();    // unchecked
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).limit(maxSize);
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).limit(maxSize);
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).limit(maxSize);
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).limit(maxSize);
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    LONG_FILTER(false, true, LongPredicate.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).filter((LongPredicate)arguments.get(0));   // unchecked
      }
    },
    LONG_FLAT_MAP(false, true, LongFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).flatMap((LongFunction)arguments.get(0));   // unchecked
      }
    },
    LONG_MAP(false, true, LongUnaryOperator.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).map((LongUnaryOperator) arguments.get(0));   // unchecked
      }
    },
    LONG_MAP_TO_DOUBLE(false, true, LongToDoubleFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).mapToDouble((LongToDoubleFunction) arguments.get(0));   // unchecked
      }
    },
    LONG_MAP_TO_INT(false, true, LongToIntFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).mapToInt((LongToIntFunction) arguments.get(0));   // unchecked
      }
    },
    LONG_MAP_TO_OBJ(false, true, LongFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((LongStream)stream).mapToObj((LongFunction)arguments.get(0));   // unchecked
      }
    },
    LONG_PEEK(LongConsumer.class),
    MAP(false, true, Function.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).map((Function)arguments.get(0));                 // unchecked
      }
    },
    MAP_TO_DOUBLE(false, true, ToDoubleFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public DoubleStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).mapToDouble((ToDoubleFunction)arguments.get(0));    // unchecked
      }
    },
    MAP_TO_INT(false, true, ToIntFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public IntStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).mapToInt((ToIntFunction)arguments.get(0));          // unchecked
      }
    },
    MAP_TO_LONG(false, true, ToLongFunction.class) {
      @SuppressWarnings("unchecked")
      @Override
      public LongStream reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).mapToLong((ToLongFunction)arguments.get(0));        // unchecked
      }
    },
    /**
     * TC Store {@code Record} update intermediate operation.
     *
     * @see com.terracottatech.store.stream.MutableRecordStream#mutateThen(UpdateOperation)
     */
    MUTATE_THEN(false, true, UpdateOperation.class) {
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        // Reconstruction of this operation must be handled specially
        throw new AssertionError("Reconstruction for " + this
            + " not supported against " + stream.getClass().getSimpleName());
      }
    },
    /**
     * Internal TC Store {@code Record} update intermediate operation.  This operation
     * replaces {@link #MUTATE_THEN} in non-portable pipeline segments.
     */
    MUTATE_THEN_INTERNAL(false, true, UpdateOperation.class) {
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        // Reconstruction of this operation must be handled specially
        throw new AssertionError("Reconstruction for " + this
            + " not supported against " + stream.getClass().getSimpleName());
      }
    },
    ON_CLOSE(Runnable.class),
    PARALLEL(false, true) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).parallel();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).parallel();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).parallel();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).parallel();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    PEEK(false, true, Consumer.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).peek((Consumer) arguments.get(0));   // unchecked
      }
    },
    SELF_CLOSE(),
    SEQUENTIAL(false, true) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).sequential();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).sequential();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).sequential();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).sequential();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    SKIP(false, true, Long.class) {
      @SuppressWarnings("unchecked")
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Long n = ((Constant<?, Long>)arguments.get(0)).getValue();    // unchecked
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).skip(n);
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).skip(n);
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).skip(n);
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).skip(n);
        } else {
          throw new AssertionError("Reconstruction for " + this
                  + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    SORTED_0(false, true) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).sorted();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).sorted();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).sorted();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).sorted();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    SORTED_1(false, true, Comparator.class) {
      @SuppressWarnings("unchecked")
      @Override
      public Stream<?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        return ((Stream<?>)stream).sorted((Comparator)arguments.get(0));       // unchecked
      }
    },
    UNORDERED(false, true) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?>  stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).unordered();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).unordered();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).unordered();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).unordered();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },

    EXPLAIN(Consumer.class);

    private final boolean supportsReconstruction;
    private final boolean shortCircuit;
    private final List<Class<?>> argumentTypes;

    IntermediateOperation(final boolean shortCircuit, final boolean supportsReconstruction, final Class<?>... argumentTypes) {
      this.supportsReconstruction = supportsReconstruction;
      this.shortCircuit = shortCircuit;
      if (argumentTypes.length == 0) {
        this.argumentTypes = Collections.emptyList();
      } else {
        this.argumentTypes = Collections.unmodifiableList(Arrays.asList(argumentTypes));
      }
    }

    IntermediateOperation(final Class<?>... argumentTypes) {
      this(false, false, argumentTypes);
    }

    @Override
    public boolean isTerminal() {
      return false;
    }

    @Override
    public boolean isShortCircuit() {
      return this.shortCircuit;
    }

    @Override
    public List<Class<?>> getArgumentTypes() {
      return this.argumentTypes;
    }

    @Override
    public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
      throw new UnsupportedOperationException("Reconstruction for " + this + " not supported");
    }

    @Override
    public boolean supportsReconstruction() {
      return this.supportsReconstruction;
    }
  }


  /**
   * Identifies a <i>terminal</i> {@code Stream} operation.
   *
   * See the <a href="doc-files/streamOperations.html#terminal">terminal operations summary</a> for a
   * list of the terminal operations showing their operand types and the Java stream implementation
   * classes with which they're used.
   *
   * <h3>Implementation Note</h3>
   * This enumeration is serialized using Runnel into messages shipped between TC Store client and server.  Care
   * must be taken when modifying this enumeration to ensure only binary-compatible changes are made.  See
   * {@code com.terracottatech.store.common.messages.StoreStructures#TERMINAL_OPERATION_ENUM_MAPPING}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public enum TerminalOperation implements Operation {
    ALL_MATCH(true, true, Predicate.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Predicate p = (Predicate) arguments.get(0);
        return ((Stream<?>)stream).allMatch(p);
      }
    },
    ANY_MATCH(true, true, Predicate.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Predicate p = (Predicate) arguments.get(0);
        return ((Stream<?>)stream).anyMatch(p);
      }
    },
    AVERAGE(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).average();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).average();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).average();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    COLLECT_1(false, true, Collector.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Collector c = (Collector) arguments.get(0);
        return ((Stream<?>)stream).collect(c);
      }
    },
    COLLECT_3(false, Supplier.class, BiConsumer.class, BiConsumer.class),
    COUNT(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).count();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).count();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).count();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).count();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    /**
     * TC Store {@code Record} delete terminal operation.
     */
    DELETE(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        // Reconstruction of this operation must be handled specially
        throw new AssertionError("Reconstruction for " + this
            + " not supported against " + stream.getClass().getSimpleName());
      }
    },
    DOUBLE_ALL_MATCH(true, DoublePredicate.class),
    DOUBLE_ANY_MATCH(true, DoublePredicate.class),
    DOUBLE_COLLECT(false, Supplier.class, ObjDoubleConsumer.class, BiConsumer.class),
    DOUBLE_FOR_EACH(false, DoubleConsumer.class),
    DOUBLE_FOR_EACH_ORDERED(false, DoubleConsumer.class),
    DOUBLE_NONE_MATCH(true, DoublePredicate.class),
    DOUBLE_REDUCE_1(false, DoubleBinaryOperator.class),
    DOUBLE_REDUCE_2(false, Double.class, DoubleBinaryOperator.class),
    FIND_ANY(true, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).findAny();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).findAny();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).findAny();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).findAny();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    FIND_FIRST(true, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof Stream) {
          return ((Stream<?>)stream).findFirst();
        } else if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).findFirst();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).findFirst();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).findFirst();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    FOR_EACH(false, Consumer.class),
    FOR_EACH_ORDERED(false, Consumer.class),
    INT_ALL_MATCH(true, IntPredicate.class),
    INT_ANY_MATCH(true, IntPredicate.class),
    INT_COLLECT(false, Supplier.class, ObjIntConsumer.class, BiConsumer.class),
    INT_FOR_EACH(false, IntConsumer.class),
    INT_FOR_EACH_ORDERED(false, IntConsumer.class),
    INT_NONE_MATCH(true, IntPredicate.class),
    INT_REDUCE_1(false, IntBinaryOperator.class),
    INT_REDUCE_2(false, Integer.class, IntBinaryOperator.class),
    ITERATOR(false),
    LONG_ALL_MATCH(true, LongPredicate.class),
    LONG_ANY_MATCH(true, LongPredicate.class),
    LONG_COLLECT(false, Supplier.class, ObjLongConsumer.class, BiConsumer.class),
    LONG_FOR_EACH(false, LongConsumer.class),
    LONG_FOR_EACH_ORDERED(false, LongConsumer.class),
    LONG_NONE_MATCH(true, LongPredicate.class),
    LONG_REDUCE_1(false, LongBinaryOperator.class),
    LONG_REDUCE_2(false, Long.class, LongBinaryOperator.class),
    MAX_0(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).max();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).max();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).max();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    MAX_1(false, true, Comparator.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Comparator c = (Comparator) arguments.get(0);
        return ((Stream<?>) stream).max(c);
      }
    },
    MIN_0(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).min();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).min();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).min();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    MIN_1(false, true, Comparator.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Comparator c = (Comparator) arguments.get(0);
        return ((Stream<?>) stream).min(c);
      }
    },
    /**
     * TC Store {@code Record} update terminal operation.
     *
     * @see com.terracottatech.store.stream.MutableRecordStream#mutate(UpdateOperation)
     */
    MUTATE(false, true, UpdateOperation.class) {
      @Override
      public BaseStream<?, ?> reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        // Reconstruction of this operation must be handled specially
        throw new AssertionError("Reconstruction for " + this
            + " not supported against " + stream.getClass().getSimpleName());
      }
    },
    NONE_MATCH(true, true, Predicate.class) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        Predicate p = (Predicate) arguments.get(0);
        return ((Stream<?>) stream).noneMatch(p);
      }
    },
    REDUCE_1(false, BinaryOperator.class),
    REDUCE_2(false, Object.class, BinaryOperator.class),
    REDUCE_3(false, Object.class, BiFunction.class, BinaryOperator.class),
    SPLITERATOR(false),
    SUM(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).sum();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).sum();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).sum();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    SUMMARY_STATISTICS(false, true) {
      @Override
      public Object reconstruct(BaseStream<?, ?> stream, List<Object> arguments) {
        if (stream instanceof DoubleStream) {
          return ((DoubleStream)stream).summaryStatistics();
        } else if (stream instanceof IntStream) {
          return ((IntStream)stream).summaryStatistics();
        } else if (stream instanceof LongStream) {
          return ((LongStream)stream).summaryStatistics();
        } else {
          throw new AssertionError("Reconstruction for " + this
              + " not supported against " + stream.getClass().getSimpleName());
        }
      }
    },
    TO_ARRAY_0(false),
    TO_ARRAY_1(false, IntFunction.class);

    private final boolean shortCircuit;
    private final List<Class<?>> argumentTypes;
    private final boolean supportsReconstruction;

    TerminalOperation(boolean shortCircuit, Class<?>... argumentTypes) {
      this(shortCircuit, false, argumentTypes);
    }

    TerminalOperation(boolean shortCircuit, boolean supportsReconstruction, Class<?>... argumentTypes) {
      this.shortCircuit = shortCircuit;
      this.supportsReconstruction = supportsReconstruction;
      if (argumentTypes.length == 0) {
        this.argumentTypes = Collections.emptyList();
      } else {
        this.argumentTypes = Collections.unmodifiableList(Arrays.asList(argumentTypes));
      }
    }

    @Override
    public boolean isTerminal() {
      return true;
    }

    @Override
    public boolean isShortCircuit() {
      return this.shortCircuit;
    }

    @Override
    public List<Class<?>> getArgumentTypes() {
      return this.argumentTypes;
    }

    @Override
    public boolean supportsReconstruction() {
      return supportsReconstruction;
    }
  }
}
