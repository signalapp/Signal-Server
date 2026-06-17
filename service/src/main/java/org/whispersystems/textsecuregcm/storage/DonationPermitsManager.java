/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Clock;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.donation.DonationPermit;
import org.signal.libsignal.zkgroup.donation.DonationPermitDerivedKeyPair;
import org.signal.libsignal.zkgroup.donation.DonationPermitRequest;
import org.signal.libsignal.zkgroup.donation.DonationPermitResponse;

public class DonationPermitsManager {

  private final DonationPermits donationPermits;
  private final ServerSecretParams serverSecretParams;
  private final Clock clock;

  public DonationPermitsManager(final DonationPermits donationPermits, final ServerSecretParams serverSecretParams,
      final Clock clock) {
    this.donationPermits = donationPermits;
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  /// Verifies and then spends the given {@link DonationPermit}
  ///
  /// @return whether the spend was successful
  /// @throws VerificationFailedException if the permit was not valid (expired or otherwise)
  public boolean spend(final DonationPermit donationPermit) throws VerificationFailedException {
    // Permits must be verified with the key pair that issued them, which can be re-derived from the embedded expiration
    final DonationPermitDerivedKeyPair issuingKeyPair = DonationPermitDerivedKeyPair.forExpiration(
        donationPermit.getExpiration(), serverSecretParams);

    donationPermit.verify(issuingKeyPair, clock.instant());

    return donationPermits.spend(donationPermit.getSpendId(), clock.instant());
  }

  /// Issues {@link DonationPermitResponse} with an expiration derived from the current time
  public DonationPermitResponse issue(final DonationPermitRequest permitRequest) {
    final DonationPermitDerivedKeyPair currentDerivedKeyPair = DonationPermitDerivedKeyPair.forExpiration(
        DonationPermitResponse.defaultExpiration(clock.instant()),
        serverSecretParams);

    return permitRequest.issue(currentDerivedKeyPair);
  }

}
