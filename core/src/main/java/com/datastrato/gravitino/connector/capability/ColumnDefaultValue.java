/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.rel.Column;

public interface ColumnDefaultValue extends Capability<Column[], Void, IllegalArgumentException> {}
