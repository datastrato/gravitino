/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.exceptions;

public class IllegalNamespaceException extends IllegalArgumentException {

  public IllegalNamespaceException(String message) {
    super(message);
  }

  public IllegalNamespaceException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalNamespaceException(Throwable cause) {
    super(cause);
  }

  public IllegalNamespaceException() {
    super();
  }
}
