/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.devicecheck.AppleDeviceCheckManager;
import org.whispersystems.textsecuregcm.storage.devicecheck.ChallengeNotFoundException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DeviceCheckKeyIdNotFoundException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DeviceCheckVerificationFailedException;
import org.whispersystems.textsecuregcm.storage.devicecheck.DuplicatePublicKeyException;
import org.whispersystems.textsecuregcm.storage.devicecheck.RequestReuseException;
import org.whispersystems.textsecuregcm.storage.devicecheck.TooManyKeysException;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
class DeviceCheckControllerTest {

  private final static Duration REDEMPTION_DURATION = Duration.ofDays(5);
  private final static long REDEMPTION_LEVEL = 201L;
  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private final static BackupAuthManager backupAuthManager = mock(BackupAuthManager.class);
  private final static AppleDeviceCheckManager appleDeviceCheckManager = mock(AppleDeviceCheckManager.class);
  private final static RateLimiters rateLimiters = mock(RateLimiters.class);
  private final static Clock clock = TestClock.pinned(Instant.EPOCH);

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new CompletionExceptionMapper())
      .addResource(new GrpcStatusRuntimeExceptionMapper())
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new DeviceCheckController(clock, accountsManager, backupAuthManager, appleDeviceCheckManager, rateLimiters,
          REDEMPTION_LEVEL, REDEMPTION_DURATION))
      .build();

  @BeforeEach
  public void setUp() {
    reset(backupAuthManager);
    reset(appleDeviceCheckManager);
    reset(rateLimiters);
    when(rateLimiters.forDescriptor(any())).thenReturn(mock(RateLimiter.class));

    when(accountsManager.getByAccountIdentifier(AuthHelper.VALID_UUID)).thenReturn(Optional.of(AuthHelper.VALID_ACCOUNT));
  }

  @ParameterizedTest
  @EnumSource(AppleDeviceCheckManager.ChallengeType.class)
  public void createChallenge(AppleDeviceCheckManager.ChallengeType challengeType) throws RateLimitExceededException {
    when(appleDeviceCheckManager.createChallenge(eq(challengeType), any()))
        .thenReturn("TestChallenge");

    WebTarget target = resources.getJerseyTest()
        .target("v1/devicecheck/%s".formatted(switch (challengeType) {
          case ATTEST -> "attest";
          case ASSERT_BACKUP_REDEMPTION -> "assert";
        }));
    if (challengeType == AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION) {
      target = target.queryParam("action", "backup");
    }
    final DeviceCheckController.ChallengeResponse challenge = target
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(DeviceCheckController.ChallengeResponse.class);

    assertThat(challenge.challenge()).isEqualTo("TestChallenge");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void createChallengeRateLimited(boolean create) throws RateLimitExceededException {
    final RateLimiter rateLimiter = mock(RateLimiter.class);
    when(rateLimiters.forDescriptor(RateLimiters.For.DEVICE_CHECK_CHALLENGE)).thenReturn(rateLimiter);
    doThrow(new RateLimitExceededException(Duration.ofSeconds(1L))).when(rateLimiter).validate(any(UUID.class));

    final String path = "v1/devicecheck/%s".formatted(create ? "assert" : "attest");

    final Response response = resources.getJerseyTest()
        .target(path)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();
    assertThat(response.getStatus()).isEqualTo(429);
  }

  @Test
  public void failedAttestValidation()
      throws DeviceCheckVerificationFailedException, ChallengeNotFoundException, TooManyKeysException, DuplicatePublicKeyException {
    final String errorMessage = "a test error message";
    final byte[] keyId = TestRandomUtil.nextBytes(16);
    final byte[] attestation = TestRandomUtil.nextBytes(32);

    doThrow(new DeviceCheckVerificationFailedException(errorMessage)).when(appleDeviceCheckManager)
        .registerAttestation(any(), eq(keyId), eq(attestation));
    final Response response = resources.getJerseyTest()
        .target("v1/devicecheck/attest")
        .queryParam("keyId", Base64.getUrlEncoder().encodeToString(keyId))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(attestation, MediaType.APPLICATION_OCTET_STREAM));

    assertThat(response.getStatus()).isEqualTo(401);
    assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_JSON_TYPE);
    assertThat(response.readEntity(Map.class).get("message")).isEqualTo(errorMessage);
  }

  @Test
  public void failedAssertValidation()
      throws DeviceCheckVerificationFailedException, ChallengeNotFoundException,  DeviceCheckKeyIdNotFoundException, RequestReuseException {
    final String errorMessage = "a test error message";
    final byte[] keyId = TestRandomUtil.nextBytes(16);
    final byte[] assertion = TestRandomUtil.nextBytes(32);
    final String challenge = "embeddedChallenge";
    final String request = """
        {"action": "backup", "challenge": "embeddedChallenge"}
        """;

    doThrow(new DeviceCheckVerificationFailedException(errorMessage)).when(appleDeviceCheckManager)
        .validateAssert(any(), eq(keyId), eq(AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION), eq(challenge), eq(request.getBytes()), eq(assertion));

    final Response response = resources.getJerseyTest()
        .target("v1/devicecheck/assert")
        .queryParam("keyId", Base64.getUrlEncoder().encodeToString(keyId))
        .queryParam("request", Base64.getUrlEncoder().encodeToString(request.getBytes(StandardCharsets.UTF_8)))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(assertion, MediaType.APPLICATION_OCTET_STREAM));

    assertThat(response.getStatus()).isEqualTo(401);
    assertThat(response.getMediaType()).isEqualTo(MediaType.APPLICATION_JSON_TYPE);
    assertThat(response.readEntity(Map.class).get("message")).isEqualTo(errorMessage);
  }

  @Test
  public void registerKey()
      throws DeviceCheckVerificationFailedException, ChallengeNotFoundException, TooManyKeysException, DuplicatePublicKeyException {
    final byte[] keyId = TestRandomUtil.nextBytes(16);
    final byte[] attestation = TestRandomUtil.nextBytes(32);
    final Response response = resources.getJerseyTest()
        .target("v1/devicecheck/attest")
        .queryParam("keyId", Base64.getUrlEncoder().encodeToString(keyId))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(attestation, MediaType.APPLICATION_OCTET_STREAM));
    assertThat(response.getStatus()).isEqualTo(204);
    verify(appleDeviceCheckManager, times(1))
        .registerAttestation(any(), eq(keyId), eq(attestation));
  }

  @Test
  public void checkAssertion()
      throws DeviceCheckKeyIdNotFoundException, DeviceCheckVerificationFailedException, ChallengeNotFoundException, RequestReuseException {
    final byte[] keyId = TestRandomUtil.nextBytes(16);
    final byte[] assertion = TestRandomUtil.nextBytes(32);
    final String challenge = "embeddedChallenge";
    final String request = """
        {"action": "backup", "challenge": "embeddedChallenge"}
        """;

    when(backupAuthManager.extendBackupVoucher(any(), eq(new Account.BackupVoucher(
        REDEMPTION_LEVEL,
        clock.instant().plus(REDEMPTION_DURATION)))))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Response response = resources.getJerseyTest()
        .target("v1/devicecheck/assert")
        .queryParam("keyId", Base64.getUrlEncoder().encodeToString(keyId))
        .queryParam("request", Base64.getUrlEncoder().encodeToString(request.getBytes(StandardCharsets.UTF_8)))
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.entity(assertion, MediaType.APPLICATION_OCTET_STREAM));
    assertThat(response.getStatus()).isEqualTo(204);
    verify(appleDeviceCheckManager, times(1)).validateAssert(
        any(),
        eq(keyId),
        eq(AppleDeviceCheckManager.ChallengeType.ASSERT_BACKUP_REDEMPTION),
        eq(challenge),
        eq(request.getBytes(StandardCharsets.UTF_8)),
        eq(assertion));
  }
}
