/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import io.grpc.Status;
import io.grpc.StatusException;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A combination of a gRPC status and response message to communicate to callers that a message has been flagged as
 * potential spam.
 *
 * @param <R> the type of response object
 */
public class GrpcResponse<R> {

  private final Status status;

  @Nullable
  private final R response;

  private GrpcResponse(final Status status, @Nullable final R response) {
    this.status = status;
    this.response = response;
  }

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
    return new GrpcResponse<>(status, null);
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
    return new GrpcResponse<>(Status.OK, response);
  }

  /**
   * Returns the message body contained within this response or throws the contained status as a {@link StatusException}
   * if no message body is specified.
   *
   * @return the message body contained within this response
   *
   * @throws StatusException if no message body is specified
   */
  public R getResponseOrThrowStatus() throws StatusException {
    if (response != null) {
      return response;
    }

    throw status.asException();
  }
}
