/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.eatthepath.otp.TimeBasedOneTimePasswordGenerator;
import com.google.protobuf.ByteString;
import com.webauthn4j.data.AttestationConveyancePreference;
import com.webauthn4j.data.AuthenticatorAttestationResponse;
import com.webauthn4j.data.AuthenticatorSelectionCriteria;
import com.webauthn4j.data.PublicKeyCredential;
import com.webauthn4j.data.PublicKeyCredentialCreationOptions;
import com.webauthn4j.data.PublicKeyCredentialParameters;
import com.webauthn4j.data.PublicKeyCredentialRpEntity;
import com.webauthn4j.data.PublicKeyCredentialType;
import com.webauthn4j.data.PublicKeyCredentialUserEntity;
import com.webauthn4j.data.ResidentKeyRequirement;
import com.webauthn4j.data.UserVerificationRequirement;
import com.webauthn4j.data.attestation.statement.COSEAlgorithmIdentifier;
import com.webauthn4j.data.client.Origin;
import com.webauthn4j.data.client.challenge.DefaultChallenge;
import com.webauthn4j.test.EmulatorUtil;
import com.webauthn4j.test.authenticator.webauthn.WebAuthnAuthenticatorAdaptor;
import com.webauthn4j.test.client.ClientPlatform;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.signal.chat.account.AccountsGrpc;
import org.signal.chat.account.ConfirmTotpKeyRequest;
import org.signal.chat.account.ConfirmTotpKeyResponse;
import org.signal.chat.account.FinishWebAuthnRegistrationRequest;
import org.signal.chat.account.FinishWebAuthnRegistrationResponse;
import org.signal.chat.account.GenerateTotpKeyRequest;
import org.signal.chat.account.GenerateTotpKeyResponse;
import org.signal.chat.account.ListMfaKeysRequest;
import org.signal.chat.account.ListMfaKeysResponse;
import org.signal.chat.account.StartWebAuthnRegistrationRequest;
import org.signal.chat.account.StartWebAuthnRegistrationResponse;
import org.signal.chat.account.TotpParameters;
import org.signal.integration.config.WebAuthnConfiguration;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.usernames.Username;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
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
  public void testCreateNumberlessAccount()
      throws VerificationFailedException, InvalidInputException {
    final Operations.Receipt receipt = Operations.getPrescribedReceipt();
    final TestUser user = Operations.registerNumberlessUser(receipt.credential());
    try {
      final Pair<Integer, AccountIdentityResponse> execute = Operations.apiGet("/v1/accounts/whoami")
          .authorized(user)
          .execute(AccountIdentityResponse.class);
      assertEquals(HttpStatus.SC_OK, execute.getLeft());
    } finally {
      Operations.deleteReceipt(receipt.serial());
      Operations.deleteUser(user);
    }
  }

  @Test
  public void testRecoverWithTotp()
      throws VerificationFailedException, InvalidInputException, NoSuchAlgorithmException, InvalidKeyException {
    final Operations.Receipt receipt = Operations.getPrescribedReceipt();
    TestUser user = Operations.registerNumberlessUser(receipt.credential());
    final UUID originalAci = user.aciUuid();

    try {
      final GenerateTotpKeyResponse generateTotpKeyResponse =
          getAccountsStubForUser(user).generateTotpKey(GenerateTotpKeyRequest.getDefaultInstance());
      assertEquals(GenerateTotpKeyResponse.ResponseCase.KEY_GENERATED, generateTotpKeyResponse.getResponseCase());

      final TotpParameters totpParameters = generateTotpKeyResponse.getKeyGenerated().getTotpParameters();
      final TimeBasedOneTimePasswordGenerator totpGenerator = new TimeBasedOneTimePasswordGenerator(
          Duration.ofSeconds(totpParameters.getTimeStepSeconds()),
          totpParameters.getPasswordLength(),
          totpParameters.getAlgorithm());

      final SecretKey totpKey = new SecretKeySpec(
          generateTotpKeyResponse.getKeyGenerated().getKey().toByteArray(),
          totpParameters.getAlgorithm());

      final byte[] totpMetadata = Operations.randomBytes(160);

      final ConfirmTotpKeyResponse confirmTotpKeyResponse = getAccountsStubForUser(user)
          .confirmTotpKey(ConfirmTotpKeyRequest.newBuilder()
              .setOneTimePassword(totpGenerator.generateOneTimePassword(totpKey, Instant.now()))
              .setMetadataCiphertext(ByteString.copyFrom(totpMetadata))
              .build());
      assertEquals(ConfirmTotpKeyResponse.ResponseCase.KEY_CONFIRMED, confirmTotpKeyResponse.getResponseCase());
      final int keyId = confirmTotpKeyResponse.getKeyConfirmed().getKeyId();

      user = Operations.recoverNumberlessUserWithTotp(user, totpGenerator.generateOneTimePassword(totpKey, Instant.now()));

      assertEquals(user.aciUuid(), originalAci);

      // MFA key should remain set after re-registration
      final Map<Integer, ListMfaKeysResponse.MfaKeyMetadata> mfaKeys =
          getAccountsStubForUser(user).listMfaKeys(ListMfaKeysRequest.getDefaultInstance()).getKeysMap();

      assertEquals(1, mfaKeys.size());
      assertTrue(mfaKeys.containsKey(keyId));
      assertEquals(ListMfaKeysResponse.MfaKeyMetadata.MfaKeyType.MFA_KEY_TYPE_TOTP, mfaKeys.get(keyId).getType());
      assertArrayEquals(totpMetadata, mfaKeys.get(keyId).getMetadataCiphertext().toByteArray());

    } finally {
      Operations.deleteReceipt(receipt.serial());
      Operations.deleteUser(user);
    }
  }

  @Test
  public void testRecoverWithWebAuthn() throws VerificationFailedException, InvalidInputException {
    final WebAuthnConfiguration webAuthnConfiguration = Operations.getWebAuthnConfiguration();
    final ClientPlatform clientPlatform = new ClientPlatform(new Origin(webAuthnConfiguration.origin()), new WebAuthnAuthenticatorAdaptor(EmulatorUtil.NONE_ATTESTATION_AUTHENTICATOR));
    final COSEAlgorithmIdentifier algorithm = COSEAlgorithmIdentifier.ES256;

    final Operations.Receipt receipt = Operations.getPrescribedReceipt();
    TestUser user = Operations.registerNumberlessUser(receipt.credential());
    final UUID originalAci = user.aciUuid();

    try {
      final StartWebAuthnRegistrationResponse startWebAuthnRegistrationResponse =
          getAccountsStubForUser(user).startWebAuthnRegistration(StartWebAuthnRegistrationRequest.getDefaultInstance());
      assertEquals(StartWebAuthnRegistrationResponse.ResponseCase.PARAMS, startWebAuthnRegistrationResponse.getResponseCase());

      final StartWebAuthnRegistrationResponse.WebAuthnCreateParameters webAuthnCreateParameters = startWebAuthnRegistrationResponse.getParams();

      assertTrue(
          webAuthnCreateParameters.getAllowedAlgorithmsList().contains(algorithm.getValue()));

      final PublicKeyCredential<AuthenticatorAttestationResponse, ?> credential =
          clientPlatform.create(new PublicKeyCredentialCreationOptions(
              new PublicKeyCredentialRpEntity(webAuthnConfiguration.relyingPartyId(), "test"),
              new PublicKeyCredentialUserEntity(webAuthnCreateParameters.getUserHandle().toByteArray(), "test", "test"),
              new DefaultChallenge(),
              List.of(new PublicKeyCredentialParameters(PublicKeyCredentialType.PUBLIC_KEY, algorithm)),
              null,
              List.of(),
              new AuthenticatorSelectionCriteria(
                  null, false, ResidentKeyRequirement.DISCOURAGED, UserVerificationRequirement.DISCOURAGED),
              AttestationConveyancePreference.NONE,
              null));

      final byte[] metadataCiphertext = Operations.randomBytes(160);

      final FinishWebAuthnRegistrationResponse finishWebAuthnRegistrationResponse = getAccountsStubForUser(user)
          .finishWebAuthnRegistration(FinishWebAuthnRegistrationRequest.newBuilder()
              .setAttestationObject(ByteString.copyFrom(credential.getResponse().getAttestationObject()))
              .setCollectedClientDataJsonBytes(ByteString.copyFrom(credential.getResponse().getClientDataJSON()))
              .setMetadataCiphertext(ByteString.copyFrom(metadataCiphertext))
              .build());
      assertEquals(FinishWebAuthnRegistrationResponse.ResponseCase.KEY_CONFIRMED, finishWebAuthnRegistrationResponse.getResponseCase());
      final int keyId = finishWebAuthnRegistrationResponse.getKeyConfirmed().getKeyId();

      user = Operations.recoverNumberlessUserWithWebAuthn(user, clientPlatform, credential.getRawId(), webAuthnConfiguration.relyingPartyId());

      assertEquals(originalAci, user.aciUuid());

      // MFA key should remain set after re-registration
      final Map<Integer, ListMfaKeysResponse.MfaKeyMetadata> mfaKeys =
          getAccountsStubForUser(user).listMfaKeys(ListMfaKeysRequest.getDefaultInstance()).getKeysMap();

      assertEquals(1, mfaKeys.size());
      assertTrue(mfaKeys.containsKey(keyId));
      assertEquals(ListMfaKeysResponse.MfaKeyMetadata.MfaKeyType.MFA_KEY_TYPE_WEBAUTHN, mfaKeys.get(keyId).getType());
      assertArrayEquals(metadataCiphertext, mfaKeys.get(keyId).getMetadataCiphertext().toByteArray());

    } finally {
      Operations.deleteReceipt(receipt.serial());
      Operations.deleteUser(user);
    }
  }

  private static AccountsGrpc.AccountsBlockingStub getAccountsStubForUser(final TestUser user) {
    return AccountsGrpc
        .newBlockingStub(Operations.grpcChannel())
        .withInterceptors(Operations.authorizationInterceptor(user, Device.PRIMARY_ID));
  }

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
  public void changePhoneNumber() {
    final TestUser user = Operations.newRegisteredUser("+19995550301");
    final String targetNumber = "+19995550302";

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();

    final ChangeNumberRequest changeNumberRequest = new ChangeNumberRequest(null,
        Operations.populateRandomRecoveryPassword(targetNumber),
        targetNumber,
        null,
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        Collections.emptyList(),
        Map.of(Device.PRIMARY_ID, Operations.generateSignedECPreKey(1, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, Operations.generateSignedKEMPreKey(2, pniIdentityKeyPair)),
        Map.of(Device.PRIMARY_ID, 17));

    try {
      Operations.clearChangeNumberWaitingPeriod(user);

      final AccountIdentityResponse accountIdentityResponse =
          Operations.apiPut("/v2/accounts/number", changeNumberRequest)
              .authorized(user)
              .executeExpectSuccess(AccountIdentityResponse.class);

      assertEquals(user.aciUuid(), accountIdentityResponse.uuid());
      assertNotEquals(user.pniUuid(), accountIdentityResponse.pni());
      assertEquals(Optional.of(targetNumber), accountIdentityResponse.number());
    } finally {
      Operations.deleteUser(user);
    }
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
