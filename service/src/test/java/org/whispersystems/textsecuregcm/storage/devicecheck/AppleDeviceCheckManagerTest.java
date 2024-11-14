/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage.devicecheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.webauthn4j.appattest.DeviceCheckManager;
import com.webauthn4j.appattest.authenticator.DCAppleDevice;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.Util;

class AppleDeviceCheckManagerTest {

  private static final UUID ACI = UUID.randomUUID();

  @RegisterExtension
  static final RedisClusterExtension CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS,
      DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS_KEY_CONSTRAINT);

  private final TestClock clock = TestClock.pinned(Instant.now());
  private AppleDeviceChecks appleDeviceChecks;
  private Account account;
  private AppleDeviceCheckManager appleDeviceCheckManager;

  @BeforeEach
  void setupDeviceChecks() {
    clock.pin(Instant.now());
    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACI);

    final DeviceCheckManager deviceCheckManager = DeviceCheckTestUtil.appleDeviceCheckManager();
    appleDeviceChecks = new AppleDeviceChecks(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DeviceCheckManager.createObjectConverter(),
        DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS.tableName(),
        DynamoDbExtensionSchema.Tables.APPLE_DEVICE_CHECKS_KEY_CONSTRAINT.tableName());
    appleDeviceCheckManager = new AppleDeviceCheckManager(appleDeviceChecks, CLUSTER_EXTENSION.getRedisCluster(),
        deviceCheckManager, DeviceCheckTestUtil.SAMPLE_TEAM_ID, DeviceCheckTestUtil.SAMPLE_BUNDLE_ID);
  }

  @Test
  public void missingChallengeAttest() {
    assertThatExceptionOfType(ChallengeNotFoundException.class).isThrownBy(() ->
        appleDeviceCheckManager.registerAttestation(account,
            DeviceCheckTestUtil.SAMPLE_KEY_ID,
            DeviceCheckTestUtil.SAMPLE_ATTESTATION));
  }

  @Test
  public void missingChallengeAssert() {
    assertThatExceptionOfType(ChallengeNotFoundException.class).isThrownBy(() ->
        appleDeviceCheckManager.validateAssert(account,
            DeviceCheckTestUtil.SAMPLE_KEY_ID,
            AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION, DeviceCheckTestUtil.SAMPLE_CHALLENGE,
            DeviceCheckTestUtil.SAMPLE_CHALLENGE.getBytes(StandardCharsets.UTF_8),
            DeviceCheckTestUtil.SAMPLE_ASSERTION));
  }

  @Test
  public void tooManyKeys() throws DuplicatePublicKeyException {
    final DCAppleDevice dcAppleDevice = DeviceCheckTestUtil.sampleDevice();

    // Fill the table with a bunch of keyIds
    final List<byte[]> keyIds = IntStream
        .range(0, AppleDeviceCheckManager.MAX_DEVICE_KEYS - 1)
        .mapToObj(i -> TestRandomUtil.nextBytes(16)).toList();
    for (byte[] keyId : keyIds) {
      appleDeviceChecks.storeAttestation(account, keyId, dcAppleDevice);
    }

    // We're allowed 1 more key for this account
    assertThatNoException().isThrownBy(() -> registerAttestation(account));

    // a new key should be rejected
    assertThatExceptionOfType(TooManyKeysException.class).isThrownBy(() ->
        appleDeviceCheckManager.registerAttestation(account,
            TestRandomUtil.nextBytes(16),
            DeviceCheckTestUtil.SAMPLE_ATTESTATION));

    // we can however accept an existing key
    assertThatNoException().isThrownBy(() -> registerAttestation(account, false));
  }

  @Test
  public void duplicateKeys() {
    assertThatNoException().isThrownBy(() -> registerAttestation(account));
    final Account duplicator = mock(Account.class);
    when(duplicator.getUuid()).thenReturn(UUID.randomUUID());

    // Both accounts use the attestation keyId, the second registration should fail
    assertThatExceptionOfType(DuplicatePublicKeyException.class)
        .isThrownBy(() -> registerAttestation(duplicator));
  }

  @Test
  public void fetchingChallengeRefreshesTtl() throws RateLimitExceededException {
    final String challenge =
        appleDeviceCheckManager.createChallenge(AppleDeviceCheckManager.ChallengeType.ATTEST, account);
    final String redisKey = AppleDeviceCheckManager.challengeKey(AppleDeviceCheckManager.ChallengeType.ATTEST,
        account.getUuid());

    final String storedChallenge = CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(cluster -> cluster.sync().get(redisKey));
    assertThat(storedChallenge).isEqualTo(challenge);

    final Supplier<Long> ttl = () -> CLUSTER_EXTENSION.getRedisCluster()
        .withCluster(cluster -> cluster.sync().ttl(redisKey));

    // Wait until the TTL visibly changes (~1sec)
    while (ttl.get() >= AppleDeviceCheckManager.CHALLENGE_TTL.toSeconds()) {
      Util.sleep(100);
    }

    // Our TTL fetch needs to happen before the TTL ticks down to make sure the TTL was actually refreshed. So it must
    // happen within 1 second. This should be plenty of time, but allow a few retries in case we get very unlucky.
    final boolean ttlRefreshed = IntStream.range(0, 5)
        .mapToObj(i -> {
          assertThatNoException()
              .isThrownBy(
                  () -> appleDeviceCheckManager.createChallenge(AppleDeviceCheckManager.ChallengeType.ATTEST, account));
          return ttl.get() == AppleDeviceCheckManager.CHALLENGE_TTL.toSeconds();
        })
        .anyMatch(detectedRefresh -> detectedRefresh);
    assertThat(ttlRefreshed).isTrue();

    assertThat(appleDeviceCheckManager.createChallenge(AppleDeviceCheckManager.ChallengeType.ATTEST, account))
        .isEqualTo(challenge);
  }

  @Test
  public void validateAssertion() {
    assertThatNoException().isThrownBy(() -> registerAttestation(account));

    // The sign counter should be 0 since we've made no attests
    assertThat(appleDeviceChecks.lookup(account, DeviceCheckTestUtil.SAMPLE_KEY_ID).get().getCounter())
        .isEqualTo(0L);

    // Rig redis to return our sample challenge for the assert
    final String assertChallengeKey = AppleDeviceCheckManager.challengeKey(
        AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION,
        account.getUuid());
    CLUSTER_EXTENSION.getRedisCluster().useCluster(cluster ->
        cluster.sync().set(assertChallengeKey, DeviceCheckTestUtil.SAMPLE_CHALLENGE));

    assertThatNoException().isThrownBy(() ->
        appleDeviceCheckManager.validateAssert(
            account,
            DeviceCheckTestUtil.SAMPLE_KEY_ID,
            AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION, DeviceCheckTestUtil.SAMPLE_CHALLENGE,
            DeviceCheckTestUtil.SAMPLE_CHALLENGE.getBytes(StandardCharsets.UTF_8),
            DeviceCheckTestUtil.SAMPLE_ASSERTION));

    CLUSTER_EXTENSION.getRedisCluster().useCluster(cluster ->
        assertThat(cluster.sync().get(assertChallengeKey)).isNull());

    // the sign counter should now be 1 (read from our sample assert)
    assertThat(appleDeviceChecks.lookup(account, DeviceCheckTestUtil.SAMPLE_KEY_ID).get().getCounter())
        .isEqualTo(1L);
  }

  @Test
  public void assertionCounterMovesBackwards() {
    assertThatNoException().isThrownBy(() -> registerAttestation(account));

    // force set the sign counter for our keyId to be larger than the sign counter in our sample assert (1)
    appleDeviceChecks.updateCounter(account, DeviceCheckTestUtil.SAMPLE_KEY_ID, 2);

    // Rig redis to return our sample challenge for the assert
    CLUSTER_EXTENSION.getRedisCluster().useCluster(cluster -> cluster.sync().set(
        AppleDeviceCheckManager.challengeKey(
            AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION,
            account.getUuid()),
        DeviceCheckTestUtil.SAMPLE_CHALLENGE));

    assertThatExceptionOfType(RequestReuseException.class).isThrownBy(() ->
        appleDeviceCheckManager.validateAssert(
            account,
            DeviceCheckTestUtil.SAMPLE_KEY_ID,
            AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION, DeviceCheckTestUtil.SAMPLE_CHALLENGE,
            DeviceCheckTestUtil.SAMPLE_CHALLENGE.getBytes(StandardCharsets.UTF_8),
            DeviceCheckTestUtil.SAMPLE_ASSERTION));
  }

  private void registerAttestation(final Account account)
      throws DeviceCheckVerificationFailedException, ChallengeNotFoundException, TooManyKeysException, DuplicatePublicKeyException {
    registerAttestation(account, true);
  }

  private void registerAttestation(final Account account, boolean assertChallengeRemoved)
      throws DeviceCheckVerificationFailedException, ChallengeNotFoundException, TooManyKeysException, DuplicatePublicKeyException {
    final String attestChallengeKey = AppleDeviceCheckManager.challengeKey(
        AppleDeviceCheckManager.ChallengeType.ATTEST,
        account.getUuid());
    CLUSTER_EXTENSION.getRedisCluster().useCluster(cluster ->
        cluster.sync().set(attestChallengeKey, DeviceCheckTestUtil.SAMPLE_CHALLENGE));
    try (MockedStatic<Instant> mocked = mockStatic(Instant.class, Mockito.CALLS_REAL_METHODS)) {
      mocked.when(Instant::now).thenReturn(DeviceCheckTestUtil.SAMPLE_TIME);
      appleDeviceCheckManager.registerAttestation(account,
          DeviceCheckTestUtil.SAMPLE_KEY_ID,
          DeviceCheckTestUtil.SAMPLE_ATTESTATION);
    }
    if (assertChallengeRemoved) {
      CLUSTER_EXTENSION.getRedisCluster().useCluster(cluster -> {
        // should be deleted once the attestation is registered
        assertThat(cluster.sync().get(attestChallengeKey)).isNull();
      });
    }
  }

}
