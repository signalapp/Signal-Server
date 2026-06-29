/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class GrpcExceptionsTest {

  @Test
  void streamClosed() {
    final ServiceIdentifier message = ServiceIdentifier.newBuilder().setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
        .setUuid(UUIDUtil.toByteString(UUID.randomUUID()))
        .build();
    final StatusRuntimeException statusRuntimeException = GrpcExceptions.streamClosed(message);
    final com.google.rpc.Status status = StatusProto.fromThrowable(statusRuntimeException);
    final List<Any> details = status.getDetailsList();
    assertEquals(2, details.size());
    assertEquals(1, details.stream().filter(any -> any.is(ErrorInfo.class)).count());

    final Any packed = details.stream().filter(any -> any.is(ServiceIdentifier.class)).findFirst().orElseThrow();
    assertEquals("type.googleapis.com/org.signal.chat.common.ServiceIdentifier", packed.getTypeUrl(), """
        The generated typeUrl for a packed Any message has changed. gRPC clients may rely on stable typeUrls
        scheme to distinguish `details` in error metadata
        """);
  }
}
