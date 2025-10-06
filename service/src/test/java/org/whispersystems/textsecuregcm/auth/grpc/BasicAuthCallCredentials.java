/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth.grpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

public class BasicAuthCallCredentials extends CallCredentials {

  private final String username;
  private final String password;

  public BasicAuthCallCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  @Override
  public void applyRequestMetadata(final RequestInfo requestInfo, final Executor appExecutor,
      final MetadataApplier applier) {
    try {
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
          HeaderUtils.basicAuthHeader(username, password));
      applier.apply(headers);
    } catch (Exception e) {
      applier.fail(Status.UNAUTHENTICATED.withCause(e));
    }
  }
}
