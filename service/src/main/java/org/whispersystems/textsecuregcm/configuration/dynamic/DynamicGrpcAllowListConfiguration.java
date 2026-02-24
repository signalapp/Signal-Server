/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration.dynamic;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/// Configure which gRPC methods are enabled
///
/// @param enableAll       enable all gRPC methods
/// @param enabledServices A list of fully qualified service names for which all RPCs should be enabled. For example,
///                        `org.signal.chat.account.AccountsAnonymous` would enable all RPCs on that service, regardless
///                        of whether the RPCs on that service appear in `enabledMethods`
/// @param enabledMethods  A list of fully qualified method names of RPCs that should be enabled. For example,
///                        `org.signal.chat.account.AccountsAnonymous/LookupUsernameHash` would enable the
///                        `LookupUsernameHash` RPC method
public record DynamicGrpcAllowListConfiguration(
    boolean enableAll,
    Set<String> enabledServices,
    Set<String> enabledMethods) {

  public DynamicGrpcAllowListConfiguration {
    if (enabledServices == null) {
      enabledServices = Collections.emptySet();
    }
    if (enabledMethods == null) {
      enabledMethods = Collections.emptySet();
    }
  }

  public DynamicGrpcAllowListConfiguration() {
    // By default, no GRPC methods are accessible
    this(false, Collections.emptySet(), Collections.emptySet());
  }
}
