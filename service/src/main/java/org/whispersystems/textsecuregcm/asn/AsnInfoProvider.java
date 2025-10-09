/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.asn;

import java.util.Optional;
import javax.annotation.Nonnull;

public interface AsnInfoProvider {

  /// Gets ASN information for an IP address.
  ///
  /// @param ipString a string representation of an IP address
  ///
  /// @return ASN information for the given IP address or empty if no ASN information was found for the given IP address
  Optional<AsnInfo> lookup(@Nonnull String ipString);

  AsnInfoProvider EMPTY = _ -> Optional.empty();
}
