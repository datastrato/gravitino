/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.auth;

import com.datastrato.gravitino.Config;

public class ServerAuthenticator {

  private Authenticator authenticator;

  private ServerAuthenticator() {}

  private static class InstanceHolder {
    private static final ServerAuthenticator INSTANCE = new ServerAuthenticator();
  }

  /**
   * Get the singleton instance of the ServerAuthenticator.
   *
   * @return The singleton instance of the ServerAuthenticator.
   */
  public static ServerAuthenticator getInstance() {
    return ServerAuthenticator.InstanceHolder.INSTANCE;
  }

  public void initialize(Config config) {
    // Create and initialize Authenticator related modules
    this.authenticator = AuthenticatorFactory.createAuthenticator(config);
    this.authenticator.initialize(config);
  }

  public Authenticator authenticator() {
    return authenticator;
  }
}
