/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

/**
 * Indicates that a caller tried to get information about the authenticated gRPC caller, but no caller has been
 * authenticated.
 */
public class NotAuthenticatedException extends Exception {
}
