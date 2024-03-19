/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.Entity;
import java.util.function.Supplier;

public interface ManagedStorage<T extends Entity>
    extends Capability<Supplier<T>, Entity, RuntimeException> {}
