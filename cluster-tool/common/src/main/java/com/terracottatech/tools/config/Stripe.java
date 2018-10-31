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
package com.terracottatech.tools.config;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


// Terracotta Stripe configuration.
public final class Stripe implements Serializable {

  private static final long serialVersionUID = 23000000000231L;
  private static final AtomicInteger STRIPE_COUNT = new AtomicInteger(0);
  private final int stripeIndex;

  public enum ConfigType {
    OFFHEAP("offheap-resources"),
    BACKUP_RESTORE("backup-restore"),
    DATA_DIRECTORIES("data-directories"),
    PLATFORM_PERSISTENCE("platform-persistence"),
    FAILOVER_PRIORITY("failover-priority"),
    WHITELIST_DEPRECATED("white-list"),
    SSL_TLS("ssl-tls"),
    WHITELIST("whitelist"),
    AUTHENTICATION("authentication"),
    AUDIT_DIRECTORY("audit-directory"),
    SECURITY_ROOT_DIRECTORY("security-root-directory");

    private final String name;
    ConfigType(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public static ConfigType of(String type) {
      for (ConfigType configType : values()) {
        if (configType.getName().equals(type)) {
          return configType;
        }
      }

      throw new IllegalArgumentException("No ConfigType found for input type: " + type);
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  public static final class Config<T> implements Serializable {

    private static final long serialVersionUID = 23000000000231L;

    private final ConfigType type;
    private final String name;
    private final T value;

    public Config(ConfigType type, String name, T value) {
      checkNonNullOrEmpty(type.getName());
      checkNonNullOrEmpty(name);
      if (value == null) {
        throw new IllegalArgumentException("Config Value cannot be null");
      }
      this.type = type;
      this.name = name;
      this.value = value;
    }

    public ConfigType getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public T getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Config<?> config = (Config<?>) o;

      if (!type.equals(config.type)) return false;
      if (!name.equals(config.name)) return false;
      return value.equals(config.value);

    }

    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + name.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "Config{" +
          "type='" + type.getName() + '\'' +
          ", name='" + name + '\'' +
          ", value=" + value +
          '}';
    }
  }

  private static void checkNonNullOrEmpty(String arg) {
    if (arg == null || arg.isEmpty()) {
      throw new IllegalArgumentException("Config cannot be null or empty");
    }
  }

  private final List<Config<?>> configs;

  // Servers within the single stripe
  private final List<Server> servers;

  private final boolean validateStrictly;

    public Stripe(List<Server> servers, Config<?>... configs) {
        this(servers, true, configs);
    }
  /**
   * Create stripe with servers, off heap memory and data root
   *
   * @param servers
   * @param validateStrictly
   * @param configs
   */
  public Stripe(List<Server> servers, boolean validateStrictly, Config<?>... configs) {
    if (servers == null) {
      throw new NullPointerException("servers cannot be null");
    }
    if (configs == null) {
      throw new NullPointerException("configs cannot be null");
    }

    if (servers.isEmpty()) {
      throw new IllegalArgumentException("At least one server should be provided.");
    }

    this.servers = servers;
    this.configs = Arrays.asList(configs);
    this.stripeIndex = STRIPE_COUNT.incrementAndGet();
    this.validateStrictly = validateStrictly;
  }

  public List<InetSocketAddress> serverInetAddresses() {
    List<InetSocketAddress> hostPorts = new ArrayList<>();
    for (Server server : servers) {
      hostPorts.add(InetSocketAddress.createUnresolved(server.getHost(), server.getPort()));
    }
    return hostPorts;
  }

  public List<Server> getServers() {
    return Collections.unmodifiableList(servers);
  }

  public List<Config<?>> getConfigs() {
    return Collections.unmodifiableList(configs);
  }

  public int getIndex() {
    return stripeIndex;
  }

  public boolean shouldValidateStrictly() {
    return validateStrictly;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Stripe stripe = (Stripe) o;
    return configs.equals(stripe.configs) && servers.equals(stripe.servers);
  }

  @Override
  public int hashCode() {
    int result = configs.hashCode();
    result = 31 * result + servers.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Stripe{" +
        "configs=" + configs +
        ", servers=" + servers +
        '}';
  }
}
