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
package com.terracottatech.test.data;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.terracottatech.store.Cell;
import com.terracottatech.store.CellCollection;
import com.terracottatech.store.definition.BoolCellDefinition;
import com.terracottatech.store.definition.BytesCellDefinition;
import com.terracottatech.store.definition.CellDefinition;
import com.terracottatech.store.CellSet;
import com.terracottatech.store.Record;
import com.terracottatech.store.definition.DoubleCellDefinition;
import com.terracottatech.store.definition.IntCellDefinition;
import com.terracottatech.store.definition.LongCellDefinition;
import com.terracottatech.store.definition.StringCellDefinition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.terracottatech.store.definition.CellDefinition.defineBool;
import static com.terracottatech.store.definition.CellDefinition.defineBytes;
import static com.terracottatech.store.definition.CellDefinition.defineDouble;
import static com.terracottatech.store.definition.CellDefinition.defineInt;
import static com.terracottatech.store.definition.CellDefinition.defineLong;
import static com.terracottatech.store.definition.CellDefinition.defineString;
import static com.terracottatech.test.data.Animals.Schema.IMAGE;
import static com.terracottatech.test.data.Animals.Schema.IS_LISTED;
import static com.terracottatech.test.data.Animals.Schema.MASS;
import static com.terracottatech.test.data.Animals.Schema.OBSERVATIONS;
import static com.terracottatech.test.data.Animals.Schema.STATUS;
import static com.terracottatech.test.data.Animals.Schema.STATUS_LEVEL;
import static com.terracottatech.test.data.Animals.Schema.TAXONOMIC_CLASS;
import static java.util.Comparator.comparing;
import static java.util.Comparator.nullsFirst;

/**
 * Data and schema for use in creating an animals dataset.
 *
 * @author Clifford W. Johnson
 */
public final class Animals {

  public static final String EXTINCT = "extinct";
  public static final String EXTINCT_IN_THE_WILD = "extinct in the wild";
  public static final String CRITICALLY_ENDANGERED = "critically endangered";
  public static final String VULNERABLE = "vulnerable";
  public static final String ENDANGERED = "endangered";
  public static final String NEAR_THREATENED = "near threatened";
  public static final String LEAST_CONCERN = "least concern";
  public static final String NOT_ASSESSED = "not fully assessed";

  public static final List<String> ORDERED_STATUS =
      Collections.unmodifiableList(Arrays.asList(
          NOT_ASSESSED, LEAST_CONCERN, NEAR_THREATENED, VULNERABLE, ENDANGERED, CRITICALLY_ENDANGERED, EXTINCT_IN_THE_WILD, EXTINCT));

  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private Animals() {
  }

  /**
   * Generator for pseudo-random values.
   */
  private static final Random RND = new Random(1L);

  /**
   * The collection of {@link Animals.Animal Animal} instances
   * that may be inserted into a dataset using {@link Animal#addTo(BiFunction)}.
   * <p>Data Source: http://www.skyenimals.com/browse_alpha.cgi
   */
  public static final List<Animal> ANIMALS = Collections.unmodifiableList(
      Arrays.asList(
          new Animal( "aardvark", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(3L) ),
          new Animal( "avocet", TAXONOMIC_CLASS.newCell("bird"), OBSERVATIONS.newCell(-1L)),
          new Animal( "axolotl", TAXONOMIC_CLASS.newCell("amphibian"), STATUS.newCell(CRITICALLY_ENDANGERED) ),
          new Animal( "bandicoot", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "barbet", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "bearded dragon", TAXONOMIC_CLASS.newCell("reptile") ),
          new Animal( "chamois", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "chimeara", TAXONOMIC_CLASS.newCell("fish") ),
          new Animal( "cormorant", TAXONOMIC_CLASS.newCell("bird"), OBSERVATIONS.newCell(2L) ),
          new Animal( "degu", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "dimetrodon", TAXONOMIC_CLASS.newCell("reptile"), STATUS.newCell(EXTINCT) ),
          new Animal( "discus", TAXONOMIC_CLASS.newCell("fish") ),
          new Animal( "echidna", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(3L) ),
          new Animal( "emu", TAXONOMIC_CLASS.newCell("bird"), OBSERVATIONS.newCell(25L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "ermine", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(1L) ),
          new Animal( "flamingo", TAXONOMIC_CLASS.newCell("bird"), OBSERVATIONS.newCell(999L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "fossa", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE) ),
          new Animal( "fulmar", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "gannet", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "gaur", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE) ),
          new Animal( "giant panda", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(ENDANGERED), OBSERVATIONS.newCell(6L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "grison", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "harrier", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "hedgehog", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(2L) ),
          new Animal( "hutia", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "ibex", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "ibis", TAXONOMIC_CLASS.newCell("bird") , OBSERVATIONS.newCell(1L) ),
          new Animal( "iguana", TAXONOMIC_CLASS.newCell("reptile"), OBSERVATIONS.newCell(50L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "jacana", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "jaguar", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(NEAR_THREATENED), OBSERVATIONS.newCell(2L) ),
          new Animal( "jellyfish", TAXONOMIC_CLASS.newCell("invertebrate"), OBSERVATIONS.newCell(1000L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "kangaroo", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(150L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "kinkajou", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(3L) ),
          new Animal( "kite", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "kiwi", TAXONOMIC_CLASS.newCell("bird"), OBSERVATIONS.newCell(7L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "koala", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(3L) ),
          new Animal( "kodkod", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE) ),
          new Animal( "kookaburra", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "lapwing", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "llama", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(100L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "loach", TAXONOMIC_CLASS.newCell("fish") ),
          new Animal( "mantis shrimp", TAXONOMIC_CLASS.newCell("invertebrate"), OBSERVATIONS.newCell(15L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "margay", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(NEAR_THREATENED) ),
          new Animal( "motmot", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "naked mole rat", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(12L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "needlefish", TAXONOMIC_CLASS.newCell("fish") ),
          new Animal( "newt", TAXONOMIC_CLASS.newCell("amphibian") ),
          new Animal( "ocelot", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "octopus", TAXONOMIC_CLASS.newCell("invertebrate"), OBSERVATIONS.newCell(4L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "orca", TAXONOMIC_CLASS.newCell("mammal"), OBSERVATIONS.newCell(1L) ),
          new Animal( "osprey", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "paca", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "parrotfish", TAXONOMIC_CLASS.newCell("fish"), OBSERVATIONS.newCell(50L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "piranha", TAXONOMIC_CLASS.newCell("fish") ),
          new Animal( "pitohui", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "potoo", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "quagga", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(EXTINCT) ),
          new Animal( "quetzal", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "quokka", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "rainbowfish", TAXONOMIC_CLASS.newCell("fish"), OBSERVATIONS.newCell(300L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "red panda", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE), OBSERVATIONS.newCell(15L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "rhea", TAXONOMIC_CLASS.newCell("bird"), STATUS.newCell(NEAR_THREATENED) ),
          new Animal( "seahorse", TAXONOMIC_CLASS.newCell("fish"), OBSERVATIONS.newCell(7L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "serval", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "squid", TAXONOMIC_CLASS.newCell("invertebrate"), OBSERVATIONS.newCell(20L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "stonefish", TAXONOMIC_CLASS.newCell("fish"), OBSERVATIONS.newCell(3L) ),
          new Animal( "takin", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE) ),
          new Animal( "tamandua", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "tanager", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "tapaculo", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "tarpon", TAXONOMIC_CLASS.newCell("fish"), OBSERVATIONS.newCell(50L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "tarsier", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "tayra", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "trogon", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "tuatara", TAXONOMIC_CLASS.newCell("reptile") ),
          new Animal( "turaco", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "urchin", TAXONOMIC_CLASS.newCell("invertebrate"), OBSERVATIONS.newCell(50L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "vanga", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "vaquita", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(CRITICALLY_ENDANGERED) ),
          new Animal( "viscacha", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "wallaby", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE), OBSERVATIONS.newCell(15L), MASS.newCell(randomMass()), IMAGE.newCell(randomImage()) ),
          new Animal( "walrus", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE), OBSERVATIONS.newCell(3L) ),
          new Animal( "wigeon", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "xenopoecilus", TAXONOMIC_CLASS.newCell("fish"), STATUS.newCell(CRITICALLY_ENDANGERED) ),
          new Animal( "xenops", TAXONOMIC_CLASS.newCell("bird") ),
          new Animal( "xerus", TAXONOMIC_CLASS.newCell("mammal") ),
          new Animal( "yak", TAXONOMIC_CLASS.newCell("mammal"), STATUS.newCell(VULNERABLE) ),
          new Animal( "zebu", TAXONOMIC_CLASS.newCell("mammal") )
      )
  );

  private static double randomMass() {
    return RND.nextDouble();
  }

  private static byte[] randomImage() {
    int size = RND.nextInt(8192);
    if (size == 0) {
      return null;
    } else {
      byte[] image = new byte[size];
      RND.nextBytes(image);
      return image;
    }
  }

  /**
   * Returns a {@link Stream Stream<Record<String>>} over {@link #ANIMALS}.
   * @return a new {@code Stream}
   */
  public static Stream<Record<String>> recordStream() {
    return ANIMALS.stream().map(AnimalRecord::new);
  }

  /**
   * Specifies the {@link CellDefinition CellDefinition} instances used
   * for describing the {@link Cell} instances used by {@link Animal#addTo(BiFunction)}.
   */
  public static final class Schema {

    /**
     * Taxonomic class cell.
     */
    public static final StringCellDefinition TAXONOMIC_CLASS = defineString("class");

    /**
     * Status cell.
     */
    public static final StringCellDefinition STATUS = defineString("status");

    /**
     * The endangered status level; absent indicates {@link #NOT_ASSESSED}.
     */
    public static final IntCellDefinition STATUS_LEVEL = defineInt("statusLevel");

    /**
     * Observation count cell.
     */
    public static final LongCellDefinition OBSERVATIONS = defineLong("observations");

    /**
     * A pseudo-randomly generated mass figure.
     */
    public static final DoubleCellDefinition MASS = defineDouble("mass");

    /**
     * A pseudo-randomly generated binary value.
     */
    public static final BytesCellDefinition IMAGE = defineBytes("image");

    /**
     * Indicates whether or not the animal is on an endangered species list whether endangered or not.
     */
    public static final BoolCellDefinition IS_LISTED = defineBool("isListed");

    private Schema() {
    }
  }

  /**
   * Data class for mapping animal records.
   */
  @SuppressWarnings("UnusedDeclaration")
  public static final class Animal implements Comparable<Animal> {
    private final String name;
    private final String taxonomicClass;
    private final String status;
    private final Integer statusLevel;
    private final Long observations;
    private final Double mass;
    private final byte[] image;
    private final boolean isListed;

    public Animal(final Record<String> record) {
      this.name = record.getKey();
      this.taxonomicClass = record.get(TAXONOMIC_CLASS).orElse(null);
      this.status = record.get(STATUS).orElse(null);
      this.statusLevel = record.get(STATUS_LEVEL).orElse(null);
      this.observations = record.get(OBSERVATIONS).orElse(null);
      this.mass = record.get(MASS).orElse(null);
      this.image = record.get(IMAGE).orElse(null);
      this.isListed = record.get(IS_LISTED).orElse(this.status != null);
    }

    public Animal(final String name, final String taxonomicClass, final String status) {
      this.name = name;
      this.taxonomicClass = taxonomicClass;
      this.status = status;
      this.statusLevel = computeStatusLevel(status);
      this.observations = null;
      this.mass = null;
      this.image = null;
      this.isListed = (status != null);
    }

    Animal(final String name, final Cell<?>... cells) {
      this.name = name;

      final Map<CellDefinition<?>, Cell<?>> cellMap = new HashMap<>();
      for (final Cell<?> cell : cells) {
        cellMap.put(cell.definition(), cell);
      }

      Cell<?> taxonomicClassCell = cellMap.get(TAXONOMIC_CLASS);
      this.taxonomicClass = taxonomicClassCell == null ? null : (String)taxonomicClassCell.value();

      Cell<?> statusCell = cellMap.get(STATUS);
      this.status = statusCell == null ? null : (String)statusCell.value();

      Cell<?> statusLevelCell = cellMap.get(STATUS_LEVEL);
      this.statusLevel = statusLevelCell == null ? computeStatusLevel(this.status) : (Integer)statusLevelCell.value();

      Cell<?> observationsCell = cellMap.get(OBSERVATIONS);
      this.observations = observationsCell == null ? null : (Long)observationsCell.value();

      Cell<?> massCell = cellMap.get(MASS);
      this.mass = massCell == null ? null : (Double)massCell.value();

      Cell<?> imageCell = cellMap.get(IMAGE);
      this.image = imageCell == null ? null : (byte[])imageCell.value();

      Cell<?> isListedCell = cellMap.get(IS_LISTED);
      this.isListed = isListedCell == null ? (this.status != null) : (Boolean)isListedCell.value();
    }

    private static Integer computeStatusLevel(String status) {
      if (status == null) {
        return null;
      } else {
        return ORDERED_STATUS.indexOf(status);
      }
    }

    public String getName() {
      return name;
    }

    public String getTaxonomicClass() {
      return taxonomicClass;
    }

    public String getStatus() {
      return status;
    }

    public Integer getStatusLevel() {
      return statusLevel;
    }

    public Long getObservations() {
      return observations;
    }

    public Double getMass() {
      return mass;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getImage() {
      return image;
    }

    public boolean isListed() {
      return isListed;
    }

    /**
     * Calls the supplied {@code BiFunction} with the key and {@link Cell} data from this {@code Animal} for the
     * purpose of adding a new {@link com.terracottatech.store.Record} to a {@link com.terracottatech.store.Dataset Dataset}.
     * @param adder the {@code BiFunction} to perform the add; the return value is expected the return value
     *              from an operation that adds a {@link com.terracottatech.store.Record} to a dataset
     * @return the value returned from {@code adder}
     */
    public <V> V addTo(BiFunction<String, CellCollection, V> adder) {
      return adder.apply(this.name, getCells());
    }

    /**
     * Converts this {@code Animal} to a {@link CellSet} of its content.  The {@code CellSet} <b>does not</b>
     * include a cell for the animal name.
     *
     * @return a new {@code CellSet} containing {@link Cell} values for this {@code Animal}
     */
    public CellSet getCells() {
      CellSet cells = new CellSet();

      if (this.taxonomicClass != null) {
        cells.add(TAXONOMIC_CLASS.newCell(this.taxonomicClass));
      }
      if (this.status != null) {
        cells.add(STATUS.newCell(this.status));
      }
      if (this.statusLevel != null) {
        cells.add(STATUS_LEVEL.newCell(statusLevel));
      }
      if (this.observations != null) {
        cells.add(OBSERVATIONS.newCell(this.observations));
      }
      if (this.mass != null) {
        cells.add(MASS.newCell(this.mass));
      }
      if (this.image != null) {
        cells.add((IMAGE.newCell(this.image)));
      }
      if (this.isListed) {
        cells.add(IS_LISTED.newCell(true));
      }
      return cells;
    }

    @Override
    public String toString() {
      @SuppressWarnings("StringBufferReplaceableByString")
      final StringBuilder sb = new StringBuilder("Animal{");
      sb.append("name='").append(name).append('\'');
      sb.append(", taxonomicClass='").append(taxonomicClass).append('\'');
      sb.append(", status='").append(status).append('\'');
      sb.append(", observations=").append(observations);
      sb.append('}');
      return sb.toString();
    }

    /**
     * {@inheritDoc}
     * Unlike {@link #compareTo(Animal)}, this method does <b>not</b> consider
     * the observation count when determining equality.
     */
    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final Animal animal = (Animal)o;

      if (!name.equals(animal.name)) return false;
      if (status != null ? !status.equals(animal.status) : animal.status != null) return false;
      if (taxonomicClass != null ? !taxonomicClass.equals(animal.taxonomicClass) : animal.taxonomicClass != null)
        return false;

      return true;
    }

    /**
     * {@inheritDoc}
     * Consistent with the {@link #equals(Object)} implementation, this method does not factor
     * the observation count into the hashCode.
     */
    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + (taxonomicClass != null ? taxonomicClass.hashCode() : 0);
      result = 31 * result + (status != null ? status.hashCode() : 0);
      return result;
    }

    /**
     * A {@link Comparator} ordering {@link Animal} instances by case-insensitive name,
     * {@link Schema#TAXONOMIC_CLASS taxonomic class}, {@link Schema#STATUS status}, and
     * {@link Schema#OBSERVATIONS observation count}.
     */
    public static final Comparator<Animal> COMPARATOR_BY_NAME =
        comparing(Animal::getName, String.CASE_INSENSITIVE_ORDER)
            .thenComparing(nullsFirst(comparing(Animal::getTaxonomicClass)))
            .thenComparing(nullsFirst(comparing(Animal::getStatus)))
            .thenComparing(nullsFirst(comparing(Animal::getObservations)));

    /**
     * A {@link Comparator} ordering {@link Animal} instances by {@link Schema#TAXONOMIC_CLASS taxonomic class},
     * case-insensitive name, {@link Schema#STATUS status}, and {@link Schema#OBSERVATIONS observation count}.
     */
    public static final Comparator<Animal> COMPARATOR_BY_CLASS =
        nullsFirst(comparing(Animal::getTaxonomicClass))
            .thenComparing(Animal::getName, String.CASE_INSENSITIVE_ORDER)
            .thenComparing(nullsFirst(comparing(Animal::getStatus)))
            .thenComparing(nullsFirst(comparing(Animal::getObservations)));

    @Override
    public int compareTo(@SuppressWarnings("NullableProblems") final Animal other) {
      Objects.requireNonNull(other);
      return COMPARATOR_BY_NAME.compare(this, other);
    }
  }

  /**
   * A {@link Record} over an {@link Animal} instance.
   */
  public static class AnimalRecord implements Record<String> {
    private final String key;
    private final CellSet cells;

    private AnimalRecord(Animals.Animal animal) {
      this.key = animal.getName();
      this.cells = animal.getCells();
    }

    public AnimalRecord(String key, Iterable<Cell<?>> cells) {
      this.key = key;
      this.cells = new CellSet(cells);
    }

    public AnimalRecord(Record<String> record) {
      this.key = record.getKey();
      this.cells = new CellSet(record);
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public <T> Optional<T> get(CellDefinition<T> cellDefinition) {
      return cells.get(cellDefinition);
    }

    @Override
    public Optional<?> get(String name) {
      return cells.get(name);
    }

    @Override
    public boolean contains(Object o) {
      return cells.contains(o);
    }

    @Override
    public Spliterator<Cell<?>> spliterator() {
      return cells.spliterator();
    }

    @Override
    public Iterator<Cell<?>> iterator() {
      return cells.iterator();
    }

    @Override
    public int size() {
      return cells.size();
    }

    @Override
    public boolean isEmpty() {
      return cells.isEmpty();
    }

    @Override
    public Object[] toArray() {
      return cells.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return cells.toArray(a);
    }

    @Override
    public boolean add(Cell<?> cell) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Cell<?>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Cell<?>> stream() {
      return cells.stream();
    }

    @Override
    public Stream<Cell<?>> parallelStream() {
      return cells.parallelStream();
    }

    @Override
    public void forEach(Consumer<? super Cell<?>> action) {
      cells.forEach(action);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AnimalRecord cells1 = (AnimalRecord)o;
      return Objects.equals(key, cells1.key) && Objects.equals(cells, cells1.cells);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, cells);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("AnimalRecord{");
      sb.append("key='").append(key).append('\'');
      sb.append(", cells=").append(cells);
      sb.append('}');
      return sb.toString();
    }
  }
}
