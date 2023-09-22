/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomUtils;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.DisabledPermittedAuthenticatedAccount;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupAuthTestUtil;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupTier;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ArchiveControllerTest {

  private static final BackupAuthManager backupAuthManager = mock(BackupAuthManager.class);
  private static final BackupManager backupManager = mock(BackupManager.class);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(Clock.systemUTC());

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(
          ImmutableSet.of(AuthenticatedAccount.class, DisabledPermittedAuthenticatedAccount.class)))
      .addProvider(new CompletionExceptionMapper())
      .addResource(new GrpcStatusRuntimeExceptionMapper())
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ArchiveController(backupAuthManager, backupManager))
      .build();

  private final UUID aci = UUID.randomUUID();
  private final byte[] backupKey = RandomUtils.nextBytes(32);

  @BeforeEach
  public void setUp() {
    reset(backupAuthManager);
    reset(backupManager);
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      GET,    v1/archives/auth/read,
      GET,    v1/archives/,
      GET,    v1/archives/upload/form,
      POST,   v1/archives/,
      PUT,    v1/archives/keys, '{"backupIdPublicKey": "aaaaa"}'
      """)
  public void anonymousAuthOnly(final String method, final String path, final String body)
      throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MEDIA, backupKey, aci);
    final Invocation.Builder request = resources.getJerseyTest()
        .target(path)
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature",
            Base64.getEncoder().encodeToString("abc".getBytes(StandardCharsets.UTF_8)));

    final Response response;
    if (body != null) {
      response = request.method(method, Entity.entity(body, MediaType.APPLICATION_JSON_TYPE));
    } else {
      response = request.method(method);
    }
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void setBackupId() throws RateLimitExceededException {
    when(backupAuthManager.commitBackupId(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/backupid")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ArchiveController.SetBackupIdRequest(backupAuthTestUtil.getRequest(backupKey, aci)),
            MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  public void setBadPublicKey() throws VerificationFailedException {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MEDIA, backupKey, aci);
    final Response response = resources.getJerseyTest()
        .target("v1/archives/keys")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.entity("""
            {"backupIdPublicKey": "aaaaa"}
            """, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  public void setPublicKey() throws VerificationFailedException {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MEDIA, backupKey, aci);
    final Response response = resources.getJerseyTest()
        .target("v1/archives/keys")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.entity(
            new ArchiveController.SetPublicKeyRequest(Curve.generateKeyPair().getPublicKey()),
            MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(204);
  }


  @ParameterizedTest
  @CsvSource(textBlock = """
      {}, 422
      '{"backupAuthCredentialRequest": "aaa"}', 400
      '{"backupAuthCredentialRequest": ""}', 400
      """)
  public void setBackupIdInvalid(final String requestBody, final int expectedStatus) {
    final Response response = resources.getJerseyTest()
        .target("v1/archives/backupid")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(requestBody, MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(expectedStatus);
  }

  public static Stream<Arguments> setBackupIdException() {
    return Stream.of(
        Arguments.of(new RateLimitExceededException(null, false), false, 429),
        Arguments.of(Status.INVALID_ARGUMENT.withDescription("async").asRuntimeException(), false, 400),
        Arguments.of(Status.INVALID_ARGUMENT.withDescription("sync").asRuntimeException(), true, 400)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void setBackupIdException(final Exception ex, final boolean sync, final int expectedStatus)
      throws RateLimitExceededException {
    if (sync) {
      when(backupAuthManager.commitBackupId(any(), any())).thenThrow(ex);
    } else {
      when(backupAuthManager.commitBackupId(any(), any())).thenReturn(CompletableFuture.failedFuture(ex));
    }
    final Response response = resources.getJerseyTest()
        .target("v1/archives/backupid")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ArchiveController.SetBackupIdRequest(backupAuthTestUtil.getRequest(backupKey, aci)),
            MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(expectedStatus);
  }

  @Test
  public void getCredentials() {
    final Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant end = start.plus(Duration.ofDays(1));
    final List<BackupAuthManager.Credential> expectedResponse = backupAuthTestUtil.getCredentials(
        BackupTier.MEDIA, backupAuthTestUtil.getRequest(backupKey, aci), start, end);
    when(backupAuthManager.getBackupAuthCredentials(any(), eq(start), eq(end))).thenReturn(
        CompletableFuture.completedFuture(expectedResponse));
    final ArchiveController.BackupAuthCredentialsResponse creds = resources.getJerseyTest()
        .target("v1/archives/auth")
        .queryParam("redemptionStartSeconds", start.getEpochSecond())
        .queryParam("redemptionEndSeconds", end.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ArchiveController.BackupAuthCredentialsResponse.class);
    assertThat(creds.credentials().get(0).redemptionTime()).isEqualTo(start.getEpochSecond());
  }

  enum BadCredentialsType {MISSING_START, MISSING_END, MISSING_BOTH}

  @ParameterizedTest
  @EnumSource
  public void getCredentialsBadInput(final BadCredentialsType badCredentialsType) {
    WebTarget builder = resources.getJerseyTest()
        .target("v1/archives/auth");

    final Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant end = start.plus(Duration.ofDays(1));
    if (badCredentialsType != BadCredentialsType.MISSING_BOTH
        && badCredentialsType != BadCredentialsType.MISSING_START) {
      builder = builder.queryParam("redemptionStartSeconds", start.getEpochSecond());
    }
    if (badCredentialsType != BadCredentialsType.MISSING_BOTH && badCredentialsType != BadCredentialsType.MISSING_END) {
      builder = builder.queryParam("redemptionEndSeconds", end.getEpochSecond());
    }
    final Response response = builder
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .method("GET");
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
  }

  @Test
  public void getBackupInfo() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupTier.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(
            new AuthenticatedBackupUser(presentation.getBackupId(), BackupTier.MEDIA)));
    when(backupManager.backupInfo(any())).thenReturn(CompletableFuture.completedFuture(new BackupManager.BackupInfo(
        1, "subdir", "filename", Optional.empty())));
    final ArchiveController.BackupInfoResponse response = resources.getJerseyTest()
        .target("v1/archives")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ArchiveController.BackupInfoResponse.class);
    assertThat(response.backupDir()).isEqualTo("subdir");
    assertThat(response.backupName()).isEqualTo("filename");
    assertThat(response.cdn()).isEqualTo(1);
    assertThat(response.usedSpace()).isNull();
  }
}
