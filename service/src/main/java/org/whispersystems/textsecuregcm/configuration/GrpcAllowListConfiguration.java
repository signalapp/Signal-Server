/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import java.util.Collections;
import java.util.List;

/// Configure which gRPC methods are enabled
///
/// @param enableAll       enable all gRPC methods
/// @param enabledServices A list of fully qualified service names for which all RPCs should be enabled. For example,
///                        `org.signal.chat.account.AccountsAnonymous` would enable all RPCs on that service, regardless
///                        of whether the RPCs on that service appear in `enabledMethods`
/// @param enabledMethods  A list of fully qualified method names of RPCs that should be enabled. For example,
///                        `org.signal.chat.account.AccountsAnonymous/LookupUsernameHash` would enable the
///                        `LookupUsernameHash` RPC method
public record GrpcAllowListConfiguration(
    boolean enableAll,
    List<String> enabledServices,
    List<String> enabledMethods) {

  public GrpcAllowListConfiguration {
    if (enabledServices == null) {
      enabledServices = Collections.emptyList();
    }
    if (enabledMethods == null) {
      enabledMethods = Collections.emptyList();
    }
  }

  public GrpcAllowListConfiguration() {
    // By default, no GRPC methods are accessible
    this(false, Collections.emptyList(), Collections.emptyList());
  }
}
