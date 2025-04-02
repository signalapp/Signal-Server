/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import io.grpc.Status;
import java.util.Optional;

/**
 * A combination of a gRPC status and response message to communicate to callers that a message has been flagged as
 * potential spam.
 *
 * @param status The gRPC status for this response. If the status is {@link Status#OK}, then a response object will be
 *               available via {@link #response}. Otherwise, callers should transmit the status as an error to clients.
 * @param response a response object to send to clients; will be present if {@link #status} is not {@link Status#OK}
 *
 * @param <R> the type of response object
 */
public record GrpcResponse<R>(Status status, Optional<R> response) {

  /**
   * Constructs a new response object with the given status and no response message.
   *
   * @param status the status to send to callers
   *
   * @return a new response object with the given status and no response message
   *
   * @param <R> the type of response object
   */
  public static <R> GrpcResponse<R> withStatus(final Status status) {
    return new GrpcResponse<>(status, Optional.empty());
  }

  /**
   * Constructs a new response object with a status of {@link Status#OK} and the given response message.
   *
   * @param response the response to send to the caller
   *
   * @return a new response object with a status of {@link Status#OK} and the given response message
   *
   * @param <R> the type of response object
   */
  public static <R> GrpcResponse<R> withResponse(final R response) {
    return new GrpcResponse<>(Status.OK, Optional.of(response));
  }
}
