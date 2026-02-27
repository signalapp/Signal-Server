/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.signal.chat.messages.ChallengeRequired;

/// A gRPC status or a challenge message to communicate to callers that a message has been flagged as potential spam.
public class GrpcChallengeResponse {

  @Nullable
  private final StatusRuntimeException statusException;

  @Nullable
  private final ChallengeRequired response;

  private GrpcChallengeResponse(final @Nullable StatusRuntimeException statusException,
                                @Nullable final ChallengeRequired response) {
    this.statusException = statusException;
    this.response = response;
    if (!((statusException == null) ^ (response == null))) {
      throw new IllegalArgumentException("exactly one of statusException and response must be non-null");
    }
  }

  /// Constructs a new response object with the given status and no challenge
  ///
  /// @param status the status to send to callers
  /// @return a new response object with the given status and no challenge
  public static GrpcChallengeResponse withStatusException(final StatusRuntimeException status) {
    return new GrpcChallengeResponse(status, null);
  }

  /// Constructs a new response object with the given challenge message.
  ///
  /// @param response the challenge message to send to the caller
  /// @return a new response object with the given challenge message
  public static GrpcChallengeResponse withResponse(final ChallengeRequired response) {
    return new GrpcChallengeResponse(null, response);
  }

  /// Returns the challenge message contained within this response or throws the contained status as a
  /// [StatusRuntimeException] if no challenge message is specified.
  ///
  /// @return the [ChallengeRequired] message
  /// @throws StatusRuntimeException if no challenge message is specified
  public ChallengeRequired getResponseOrThrowStatus() throws StatusRuntimeException {
    if (statusException != null) {
      throw statusException;
    }
    return response;
  }

  /// If this response contains a challenge message, throw a status generated using statusMapper. Otherwise, throw the
  /// status.
  ///
  /// @param statusMapper A function that converts a [ChallengeRequired] message into a
  ///                     [StatusRuntimeException]
  /// @throws StatusRuntimeException the contained or mapped status exception
  public void throwStatusOr(Function<ChallengeRequired, StatusRuntimeException> statusMapper)
      throws StatusRuntimeException {
    if (statusException != null) {
      throw statusException;
    }
    throw statusMapper.apply(response);
  }
}
