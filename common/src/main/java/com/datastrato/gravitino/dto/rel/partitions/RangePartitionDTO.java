/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.apache.gravitino.rel.partitions.RangePartition;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import java.util.Map;
import java.util.Objects;

/** Data transfer object representing a range partition. */
public class RangePartitionDTO implements PartitionDTO, RangePartition {

  private final String name;
  private final Map<String, String> properties;
  private final LiteralDTO upper;
  private final LiteralDTO lower;

  /** @return A builder instance for {@link RangePartitionDTO}. */
  public static Builder builder() {
    return new Builder();
  }

  private RangePartitionDTO() {
    this(null, null, null, null);
  }

  private RangePartitionDTO(
      String name, Map<String, String> properties, LiteralDTO upper, LiteralDTO lower) {
    this.name = name;
    this.properties = properties;
    this.upper = upper;
    this.lower = lower;
  }

  /** @return The name of the partition. */
  @Override
  public String name() {
    return name;
  }

  /** @return The upper bound. */
  @Override
  public LiteralDTO upper() {
    return upper;
  }

  /** @return The lower bound. */
  @Override
  public LiteralDTO lower() {
    return lower;
  }

  /** @return The properties. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RangePartitionDTO)) {
      return false;
    }
    RangePartitionDTO that = (RangePartitionDTO) o;
    return Objects.equals(name, that.name)
        && Objects.equals(properties, that.properties)
        && Objects.equals(upper, that.upper)
        && Objects.equals(lower, that.lower);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties, upper, lower);
  }

  /** @return The type of the partition. */
  @Override
  public Type type() {
    return Type.RANGE;
  }

  /** Builder for {@link RangePartitionDTO}. */
  public static class Builder {
    private String name;
    private Map<String, String> properties;
    private LiteralDTO upper;
    private LiteralDTO lower;

    /**
     * Set the name for the partition.
     *
     * @param name The name.
     * @return The builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the properties for the partition.
     *
     * @param properties The properties.
     * @return The builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Set the upper bound for the partition.
     *
     * @param upper The upper bound.
     * @return The builder.
     */
    public Builder withUpper(LiteralDTO upper) {
      this.upper = upper;
      return this;
    }

    /**
     * Set the lower bound for the partition.
     *
     * @param lower The lower bound.
     * @return The builder.
     */
    public Builder withLower(LiteralDTO lower) {
      this.lower = lower;
      return this;
    }

    /** @return The range partition instance. */
    public RangePartitionDTO build() {
      return new RangePartitionDTO(name, properties, upper, lower);
    }
  }
}
