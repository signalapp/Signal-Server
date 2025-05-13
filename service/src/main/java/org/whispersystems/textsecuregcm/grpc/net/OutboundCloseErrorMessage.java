/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

/**
 * An error written to the outbound pipeline that indicates the connection should be closed
 */
public record OutboundCloseErrorMessage(Code code, String message) {
  public enum Code {

    /**
     * The server decided to close the connection. This could be because the server is going away, or it could be
     * because the credentials for the connected client have been updated.
     */
    SERVER_CLOSED,

    /**
     * There was a noise decryption error after the noise session was established
     */
    NOISE_ERROR,

    /**
     * There was an error establishing the noise handshake
     */
    NOISE_HANDSHAKE_ERROR,

    INTERNAL_SERVER_ERROR
  }
}
