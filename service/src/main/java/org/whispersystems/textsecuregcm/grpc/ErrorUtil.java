/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import java.io.UncheckedIOException;
import java.util.Optional;

public class ErrorUtil {
  public static Optional<ErrorInfo> errorInfo(final com.google.rpc.Status statusProto) {
    return statusProto.getDetailsList().stream()
        .filter(any -> any.is(ErrorInfo.class))
        .map(errorInfo -> {
          try {
            return errorInfo.unpack(ErrorInfo.class);
          } catch (final InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
          }
        })
        .findFirst();
  }
}
