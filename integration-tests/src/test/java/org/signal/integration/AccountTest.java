/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.usernames.Username;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ChangeNumberRequest;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameHashResponse;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Device;

public class AccountTest {

  @Test
  public void testCreateAccount() {
    final TestUser user = Operations.newRegisteredUser("+19995550101");
    try {
      final Pair<Integer, AccountIdentityResponse> execute = Operations.apiGet("/v1/accounts/whoami")
          .authorized(user)
          .execute(AccountIdentityResponse.class);
      assertEquals(HttpStatus.SC_OK, execute.getLeft());
    } finally {
      Operations.deleteUser(user);
    }
  }

  @Test
  public void testCreateAccountAtomic() {
    final TestUser user = Operations.newRegisteredUser("+19995550201");
    try {
      final Pair<Integer, AccountIdentityResponse> execute = Operations.apiGet("/v1/accounts/whoami")
          .authorized(user)
          .execute(AccountIdentityResponse.class);
      assertEquals(HttpStatus.SC_OK, execute.getLeft());
    } finally {
      Operations.deleteUser(user);
    }
  }

  @Test
  public void changePhoneNumber() {
    final TestUser user = Operations.newRegisteredUser("+19995550301");
    final String targetNumber = "+19995550302";

    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    final ChangeNumberRequest changeNumberRequest = new ChangeNumberRequest(null,
        Operations.populateRandomRecoveryPassword(targetNumber),
        targetNumber,
        null,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Collections.emptyList(),
        Map.of(Device.PRIMARY_ID, Operations.generateSignedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, Operations.generateSignedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 17));

    final AccountIdentityResponse accountIdentityResponse =
        Operations.apiPut("/v2/accounts/number", changeNumberRequest)
            .authorized(user)
            .executeExpectSuccess(AccountIdentityResponse.class);

    assertEquals(user.aciUuid(), accountIdentityResponse.uuid());
    assertNotEquals(user.pniUuid(), accountIdentityResponse.pni());
    assertEquals(targetNumber, accountIdentityResponse.number());
  }

  @Test
  public void testUsernameOperations() throws Exception {
    final TestUser user = Operations.newRegisteredUser("+19995550102");
    try {
      verifyFullUsernameLifecycle(user);
      // no do it again to check changing usernames
      verifyFullUsernameLifecycle(user);
    } finally {
      Operations.deleteUser(user);
    }
  }

  private static void verifyFullUsernameLifecycle(final TestUser user) throws BaseUsernameException {
    final String preferred = "test";
    final List<Username> candidates = Username.candidatesFrom(preferred, preferred.length(), preferred.length() + 1);

    // reserve a username
    final ReserveUsernameHashRequest reserveUsernameHashRequest = new ReserveUsernameHashRequest(
        candidates.stream().map(Username::getHash).toList());
    // try unauthorized
    Operations
        .apiPut("/v1/accounts/username_hash/reserve", reserveUsernameHashRequest)
        .executeExpectStatusCode(HttpStatus.SC_UNAUTHORIZED);

    final ReserveUsernameHashResponse reserveUsernameHashResponse = Operations
        .apiPut("/v1/accounts/username_hash/reserve", reserveUsernameHashRequest)
        .authorized(user)
        .executeExpectSuccess(ReserveUsernameHashResponse.class);

    // find which one is the reserved username
    final byte[] reservedHash = reserveUsernameHashResponse.usernameHash();
    final Username reservedUsername = candidates.stream()
        .filter(u -> Arrays.equals(u.getHash(), reservedHash))
        .findAny()
        .orElseThrow();

    // confirm a username
   final ConfirmUsernameHashRequest confirmUsernameHashRequest = new ConfirmUsernameHashRequest(
        reservedUsername.getHash(),
        reservedUsername.generateProof(),
        "cluck cluck i'm a parrot".getBytes()
    );
    // try unauthorized
    Operations
        .apiPut("/v1/accounts/username_hash/confirm", confirmUsernameHashRequest)
        .executeExpectStatusCode(HttpStatus.SC_UNAUTHORIZED);
    Operations
        .apiPut("/v1/accounts/username_hash/confirm", confirmUsernameHashRequest)
        .authorized(user)
        .executeExpectSuccess(UsernameHashResponse.class);


    // lookup username
    final AccountIdentifierResponse accountIdentifierResponse = Operations
        .apiGet("/v1/accounts/username_hash/" + Base64.getUrlEncoder().encodeToString(reservedHash))
        .executeExpectSuccess(AccountIdentifierResponse.class);
    assertEquals(new AciServiceIdentifier(user.aciUuid()), accountIdentifierResponse.uuid());
    // try authorized
    Operations
        .apiGet("/v1/accounts/username_hash/" + Base64.getUrlEncoder().encodeToString(reservedHash))
        .authorized(user)
        .executeExpectStatusCode(HttpStatus.SC_BAD_REQUEST);

    // delete username
    Operations
        .apiDelete("/v1/accounts/username_hash")
        .authorized(user)
        .executeExpectSuccess();
  }
}
