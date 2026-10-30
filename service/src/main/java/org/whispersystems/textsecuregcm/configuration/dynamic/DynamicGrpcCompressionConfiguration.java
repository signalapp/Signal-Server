/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration.dynamic;

import java.util.Collections;
import java.util.Set;

/// Configure which gRPC methods may compress their responses
///
/// @param compressedServices A list of fully qualified service names for which all RPC responses should be compressed.
///                           For example, `kt_query.KeyTransparencyQueryService` would compress responses for all RPCs
///                           on that service.
/// @param compressedMethods  A list of fully qualified method names of RPCs whose responses should be compressed. For
///                           example, `kt_query.KeyTransparencyQueryService/Search` would compress responses for the
///                           `Search` RPC method
public record DynamicGrpcCompressionConfiguration(
    Set<String> compressedServices,
    Set<String> compressedMethods) {

  public DynamicGrpcCompressionConfiguration {
    if (compressedServices == null) {
      compressedServices = Collections.emptySet();
    }
    if (compressedMethods == null) {
      compressedMethods = Collections.emptySet();
    }
  }

  public DynamicGrpcCompressionConfiguration() {
    // By default, no gRPC responses are compressed
    this(Collections.emptySet(), Collections.emptySet());
  }
}
