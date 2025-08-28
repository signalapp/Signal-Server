/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import com.google.common.annotations.VisibleForTesting;
import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.authenticator.DCAppleDevice;
import com.webauthn4j.appattest.authenticator.DCAppleDeviceImpl;
import com.webauthn4j.appattest.data.DCAssertionParameters;
import com.webauthn4j.appattest.data.DCAssertionRequest;
import com.webauthn4j.appattest.data.DCAttestationData;
import com.webauthn4j.appattest.data.DCAttestationParameters;
import com.webauthn4j.appattest.data.DCAttestationRequest;
import com.webauthn4j.appattest.server.DCServerProperty;
import com.webauthn4j.data.attestation.AttestationObject;
import com.webauthn4j.data.client.challenge.DefaultChallenge;
import com.webauthn4j.verifier.exception.MaliciousCounterValueException;
import com.webauthn4j.verifier.exception.VerificationException;
import io.lettuce.core.RedisException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

/**
 * Register Apple DeviceCheck App Attestations and verify the corresponding assertions.
 *
 * @see <a href="https://developer.apple.com/documentation/devicecheck/establishing-your-app-s-integrity">...</a>
 * @see <a
 * href="https://developer.apple.com/documentation/devicecheck/validating-apps-that-connect-to-your-server">...</a>
 */
public class AppleDeviceCheckManager {

  private static final Logger logger = LoggerFactory.getLogger(AppleDeviceCheckManager.class);

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final int CHALLENGE_LENGTH = 16;

  // How long issued challenges last in redis
  @VisibleForTesting
  static final Duration CHALLENGE_TTL = Duration.ofHours(1);

  // How many distinct device keys we're willing to accept for a single Account
  @VisibleForTesting
  static final int MAX_DEVICE_KEYS = 100;

  private final AppleDeviceChecks appleDeviceChecks;
  private final FaultTolerantRedisClusterClient redisClient;
  private final DeviceCheckManager deviceCheckManager;
  private final String teamId;
  private final String bundleId;

  private static final String RETRY_NAME = ResilienceUtil.name(AppleDeviceCheckManager.class);

  public AppleDeviceCheckManager(
      AppleDeviceChecks appleDeviceChecks,
      FaultTolerantRedisClusterClient redisClient,
      DeviceCheckManager deviceCheckManager,
      String teamId,
      String bundleId) {
    this.appleDeviceChecks = appleDeviceChecks;
    this.redisClient = redisClient;
    this.deviceCheckManager = deviceCheckManager;
    this.teamId = teamId;
    this.bundleId = bundleId;
  }

  /**
   * Attestations and assertions have independent challenges.
   * <p>
   * Challenges are tied to their purpose to mitigate replay attacks
   */
  public enum ChallengeType {
    ATTEST,
    ASSERT_BACKUP_REDEMPTION
  }

  /**
   * Register a key and attestation data for an account
   *
   * @param account    The account this keyId should be associated with
   * @param keyId      The device's keyId
   * @param attestBlob The device's attestation
   * @throws ChallengeNotFoundException             No issued challenge found for the account
   * @throws DeviceCheckVerificationFailedException The provided attestation could not be verified
   * @throws TooManyKeysException                   The account has registered too many unique keyIds
   * @throws DuplicatePublicKeyException            The keyId has already been used with another account
   */
  public void registerAttestation(final Account account, final byte[] keyId, final byte[] attestBlob)
      throws TooManyKeysException, ChallengeNotFoundException, DeviceCheckVerificationFailedException, DuplicatePublicKeyException {

    final List<byte[]> existingKeys = appleDeviceChecks.keyIds(account);
    if (existingKeys.stream().anyMatch(x -> MessageDigest.isEqual(x, keyId))) {
      // We already have the key, so no need to continue
      return;
    }

    if (existingKeys.size() >= MAX_DEVICE_KEYS) {
      // This is best-effort, since we don't check the number of keys transactionally. We just don't want to allow
      // the keys for an account to grow arbitrarily large
      throw new TooManyKeysException();
    }

    final String redisChallengeKey = challengeKey(ChallengeType.ATTEST, account.getUuid());

    @Nullable final String challenge = ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeSupplier(() -> redisClient.withCluster(cluster -> cluster.sync().get(redisChallengeKey)));

    if (challenge == null) {
      throw new ChallengeNotFoundException();
    }

    final byte[] clientDataHash = sha256(challenge.getBytes(StandardCharsets.UTF_8));
    final DCAttestationRequest dcAttestationRequest = new DCAttestationRequest(keyId, attestBlob, clientDataHash);
    final DCAttestationData dcAttestationData;
    try {
      dcAttestationData = deviceCheckManager.validate(dcAttestationRequest,
          new DCAttestationParameters(new DCServerProperty(teamId, bundleId, new DefaultChallenge(challenge))));
    } catch (VerificationException e) {
      logger.info("Failed to verify attestation", e);
      throw new DeviceCheckVerificationFailedException(e);
    }
    appleDeviceChecks.storeAttestation(account, keyId, createDcAppleDevice(dcAttestationData));
    removeChallenge(redisChallengeKey);
  }

  private static DCAppleDeviceImpl createDcAppleDevice(final DCAttestationData dcAttestationData) {
    final AttestationObject attestationObject = dcAttestationData.getAttestationObject();
    if (attestationObject == null || attestationObject.getAuthenticatorData().getAttestedCredentialData() == null) {
      throw new IllegalArgumentException("Signed and validated attestation missing expected data");
    }
    return new DCAppleDeviceImpl(
        attestationObject.getAuthenticatorData().getAttestedCredentialData(),
        attestationObject.getAttestationStatement(),
        attestationObject.getAuthenticatorData().getSignCount(),
        attestationObject.getAuthenticatorData().getExtensions());
  }

  /**
   * Validate that a request came from an Apple device signed with a key already registered to the account
   *
   * @param account       The requesting account
   * @param keyId         The key used to generate the assertion
   * @param challengeType The {@link ChallengeType} of the assertion, which must match the challenge returned by
   *                      {@link AppleDeviceCheckManager#createChallenge}
   * @param challenge     A challenge that was embedded in the supplied request
   * @param request       The request that the client asserted
   * @param assertion     The assertion from the client
   * @throws DeviceCheckKeyIdNotFoundException      The provided keyId was never registered with the account
   * @throws ChallengeNotFoundException             No issued challenge found for the account
   * @throws DeviceCheckVerificationFailedException The provided assertion could not be verified
   * @throws RequestReuseException                  The signed counter on the assertion was lower than a previously
   *                                                received assertion
   */
  public void validateAssert(
      final Account account,
      final byte[] keyId,
      final ChallengeType challengeType,
      final String challenge,
      final byte[] request,
      final byte[] assertion)
      throws ChallengeNotFoundException, DeviceCheckVerificationFailedException, DeviceCheckKeyIdNotFoundException, RequestReuseException {

    final String redisChallengeKey = challengeKey(challengeType, account.getUuid());
    @Nullable final String storedChallenge = ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
            .executeSupplier(() -> redisClient.withCluster(cluster -> cluster.sync().get(redisChallengeKey)));

    if (storedChallenge == null) {
      throw new ChallengeNotFoundException();
    }
    if (!MessageDigest.isEqual(
        storedChallenge.getBytes(StandardCharsets.UTF_8),
        challenge.getBytes(StandardCharsets.UTF_8))) {
      throw new DeviceCheckVerificationFailedException("Provided challenge did not match stored challenge");
    }

    final DCAppleDevice appleDevice = appleDeviceChecks.lookup(account, keyId)
        .orElseThrow(DeviceCheckKeyIdNotFoundException::new);
    final DCAssertionRequest dcAssertionRequest = new DCAssertionRequest(keyId, assertion, sha256(request));
    final DCAssertionParameters dcAssertionParameters =
        new DCAssertionParameters(new DCServerProperty(teamId, bundleId, new DefaultChallenge(request)), appleDevice);

    try {
      deviceCheckManager.validate(dcAssertionRequest, dcAssertionParameters);
    } catch (MaliciousCounterValueException e) {
      // We will only accept assertions that have a sign count greater than the last assertion we saw. Step 5 here:
      // https://developer.apple.com/documentation/devicecheck/validating-apps-that-connect-to-your-server#Verify-the-assertion
      throw new RequestReuseException("Sign count from request less than stored sign count");
    } catch (VerificationException e) {
      logger.info("Failed to validate DeviceCheck assert", e);
      throw new DeviceCheckVerificationFailedException(e);
    }

    // Store the updated sign count, so we can check the next assertion (step 6)
    appleDeviceChecks.updateCounter(account, keyId, appleDevice.getCounter());
    removeChallenge(redisChallengeKey);
  }

  /**
   * Create a challenge that can be used in an attestation or assertion
   *
   * @param challengeType The type of the challenge
   * @param account       The account that will use the challenge
   * @return The challenge to be included as part of an attestation or assertion
   */
  public String createChallenge(final ChallengeType challengeType, final Account account) {
    final UUID accountIdentifier = account.getUuid();

    final String challengeKey = challengeKey(challengeType, accountIdentifier);
    return ResilienceUtil.getGeneralRedisRetry(RETRY_NAME)
        .executeSupplier(() -> redisClient.withCluster(cluster -> {
          final RedisAdvancedClusterCommands<String, String> commands = cluster.sync();

          // Sets the new challenge if and only if there isn't already one stored for the challenge key; returns the existing
          // challenge if present or null if no challenge was previously set.
          final String proposedChallenge = generateChallenge();
          @Nullable final String existingChallenge =
              commands.setGet(challengeKey, proposedChallenge, SetArgs.Builder.nx().ex(CHALLENGE_TTL));

          if (existingChallenge != null) {
            // If the key was already set, make sure we extend the TTL. This is racy because the key could disappear or have
            // been updated since the get returned, but it's fine. In the former case, this is a noop. In the latter
            // case we may slightly extend the TTL from after it was set, but that's also no big deal.
            commands.expire(challengeKey, CHALLENGE_TTL);
          }

          return existingChallenge != null ? existingChallenge : proposedChallenge;
        }));
  }

  private void removeChallenge(final String challengeKey) {
    try {
      redisClient.useCluster(cluster -> cluster.sync().del(challengeKey));
    } catch (RedisException e) {
      logger.debug("failed to remove attest challenge from redis, will let it expire via TTL");
    }
  }

  @VisibleForTesting
  static String challengeKey(final ChallengeType challengeType, final UUID accountIdentifier) {
    return "device_check::" + challengeType.name() + "::" + accountIdentifier.toString();
  }

  private static String generateChallenge() {
    final byte[] challenge = new byte[CHALLENGE_LENGTH];
    SECURE_RANDOM.nextBytes(challenge);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(challenge);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("All Java implementations are required to support SHA-256", e);
    }
  }
}
