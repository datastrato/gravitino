/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.exceptions;

public class IllegalNameIdentifierException extends IllegalArgumentException {

  public IllegalNameIdentifierException(String message) {
    super(message);
  }

  public IllegalNameIdentifierException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalNameIdentifierException(Throwable cause) {
    super(cause);
  }

  public IllegalNameIdentifierException() {
    super();
  }
}
