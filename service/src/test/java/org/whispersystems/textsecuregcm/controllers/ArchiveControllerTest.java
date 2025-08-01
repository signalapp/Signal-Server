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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupAuthTestUtil;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.BackupUploadDescriptor;
import org.whispersystems.textsecuregcm.backup.CopyResult;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.EnumMapUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ArchiveControllerTest {

  private static final AccountsManager accountsManager = mock(AccountsManager.class);
  private static final BackupAuthManager backupAuthManager = mock(BackupAuthManager.class);
  private static final BackupManager backupManager = mock(BackupManager.class);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(Clock.systemUTC());

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .addProvider(new CompletionExceptionMapper())
      .addResource(new GrpcStatusRuntimeExceptionMapper())
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ArchiveController(accountsManager, backupAuthManager, backupManager, new BackupMetrics()))
      .build();

  private final UUID aci = UUID.randomUUID();
  private final byte[] messagesBackupKey = TestRandomUtil.nextBytes(32);
  private final byte[] mediaBackupKey = TestRandomUtil.nextBytes(32);

  @BeforeEach
  public void setUp() {
    reset(backupAuthManager);
    reset(backupManager);

    when(accountsManager.getByAccountIdentifierAsync(AuthHelper.VALID_UUID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(AuthHelper.VALID_ACCOUNT)));
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      GET,    v1/archives/auth/read,
      GET,    v1/archives/auth/svrb,
      GET,    v1/archives/,
      GET,    v1/archives/upload/form,
      GET,    v1/archives/media/upload/form,
      POST,   v1/archives/,
      PUT,    v1/archives/keys, '{"backupIdPublicKey": "aaaaa"}'
      DELETE, v1/archives,
      PUT,    v1/archives/media, '{
        "sourceAttachment": {"cdn": 3, "key": "abc"},
        "objectLength": 10,
        "mediaId": "aaaaaaaaaaaaaaaaaaaa",
        "hmacKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "encryptionKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "iv": "aaaaaaaaaaaaaaaaaaaaaa"
      }'
      PUT,    v1/archives/media/batch, '{"items": [{
        "sourceAttachment": {"cdn": 3, "key": "abc"},
        "objectLength": 10,
        "mediaId": "aaaaaaaaaaaaaaaaaaaa",
        "hmacKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "encryptionKey": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "iv": "aaaaaaaaaaaaaaaaaaaaaa"
      }]}'
      """)
  public void anonymousAuthOnly(final String method, final String path, final String body)
      throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
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
  public void setBackupId() {
    when(backupAuthManager.commitBackupId(any(), any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/backupid")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ArchiveController.SetBackupIdRequest(
                backupAuthTestUtil.getRequest(messagesBackupKey, aci),
                backupAuthTestUtil.getRequest(mediaBackupKey, aci)),
            MediaType.APPLICATION_JSON_TYPE));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(backupAuthManager).commitBackupId(AuthHelper.VALID_ACCOUNT, AuthHelper.VALID_DEVICE,
        backupAuthTestUtil.getRequest(messagesBackupKey, aci),
        backupAuthTestUtil.getRequest(mediaBackupKey, aci));
  }

  @Test
  public void redeemReceipt() throws InvalidInputException, VerificationFailedException {
    final ServerSecretParams params = ServerSecretParams.generate();
    final ServerZkReceiptOperations serverOps = new ServerZkReceiptOperations(params);
    final ClientZkReceiptOperations clientOps = new ClientZkReceiptOperations(params.getPublicParams());
    final ReceiptCredentialRequestContext rcrc = clientOps
        .createReceiptCredentialRequestContext(new ReceiptSerial(TestRandomUtil.nextBytes(ReceiptSerial.SIZE)));
    final ReceiptCredentialResponse rcr = serverOps.issueReceiptCredential(rcrc.getRequest(), 0L, 3L);
    final ReceiptCredential receiptCredential = clientOps.receiveReceiptCredential(rcrc, rcr);
    final ReceiptCredentialPresentation presentation = clientOps.createReceiptCredentialPresentation(receiptCredential);
    when(backupAuthManager.redeemReceipt(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/redeem-receipt")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .post(Entity.json("""
            {"receiptCredentialPresentation": "%s"}
            """.formatted(Base64.getEncoder().encodeToString(presentation.serialize()))));
    assertThat(response.getStatus()).isEqualTo(204);
  }


  @Test
  public void setBadPublicKey() throws VerificationFailedException {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
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
  public void setMissingPublicKey() throws VerificationFailedException {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    final Response response = resources.getJerseyTest()
        .target("v1/archives/keys")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  public void setPublicKey() throws VerificationFailedException {
    when(backupManager.setPublicKey(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    final Response response = resources.getJerseyTest()
        .target("v1/archives/keys")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.entity(
            new ArchiveController.SetPublicKeyRequest(ECKeyPair.generate().getPublicKey()),
            MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(204);
  }


  @ParameterizedTest
  @CsvSource(textBlock = """
      {}, 422
      '{"messagesBackupAuthCredentialRequest": "aaa", "mediaBackupAuthCredentialRequest": "aaa"}', 400
      '{"messagesBackupAuthCredentialRequest": "", "mediaBackupAuthCredentialRequest": ""}', 400
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
        Arguments.of(new RateLimitExceededException(null), false, 429),
        Arguments.of(Status.INVALID_ARGUMENT.withDescription("async").asRuntimeException(), false, 400),
        Arguments.of(Status.INVALID_ARGUMENT.withDescription("sync").asRuntimeException(), true, 400)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void setBackupIdException(final Exception ex, final boolean sync, final int expectedStatus) {
    if (sync) {
      when(backupAuthManager.commitBackupId(any(), any(), any(), any())).thenThrow(ex);
    } else {
      when(backupAuthManager.commitBackupId(any(), any(), any(), any())).thenReturn(CompletableFuture.failedFuture(ex));
    }
    final Response response = resources.getJerseyTest()
        .target("v1/archives/backupid")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new ArchiveController.SetBackupIdRequest(
                backupAuthTestUtil.getRequest(messagesBackupKey, aci),
                backupAuthTestUtil.getRequest(mediaBackupKey, aci)),
            MediaType.APPLICATION_JSON_TYPE));
    assertThat(response.getStatus()).isEqualTo(expectedStatus);
  }

  @Test
  public void getCredentials() {
    final Instant start = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final Instant end = start.plus(Duration.ofDays(1));

    final Map<BackupCredentialType, List<BackupAuthManager.Credential>> expectedCredentialsByType =
        EnumMapUtil.toEnumMap(BackupCredentialType.class, credentialType -> backupAuthTestUtil.getCredentials(
            BackupLevel.PAID, backupAuthTestUtil.getRequest(messagesBackupKey, aci), credentialType, start, end));

    expectedCredentialsByType.forEach((credentialType, expectedCredentials) ->
        when(backupAuthManager.getBackupAuthCredentials(any(), eq(credentialType), eq(start), eq(end))).thenReturn(
        CompletableFuture.completedFuture(expectedCredentials)));

    final ArchiveController.BackupAuthCredentialsResponse credentialResponse = resources.getJerseyTest()
        .target("v1/archives/auth")
        .queryParam("redemptionStartSeconds", start.getEpochSecond())
        .queryParam("redemptionEndSeconds", end.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ArchiveController.BackupAuthCredentialsResponse.class);

    expectedCredentialsByType.forEach((libsignalCredentialType, expectedCredentials) -> {
      final ArchiveController.BackupAuthCredentialsResponse.CredentialType credentialType =
          ArchiveController.BackupAuthCredentialsResponse.CredentialType.fromLibsignalType(libsignalCredentialType);
      assertThat(credentialResponse.credentials().get(credentialType)).size().isEqualTo(expectedCredentials.size());
      assertThat(credentialResponse.credentials().get(credentialType).getFirst().redemptionTime())
          .isEqualTo(start.getEpochSecond());

      for (int i = 0; i < expectedCredentials.size(); i++) {
        assertThat(credentialResponse.credentials().get(credentialType).get(i).redemptionTime())
            .isEqualTo(expectedCredentials.get(i).redemptionTime().getEpochSecond());

        assertThat(credentialResponse.credentials().get(credentialType).get(i).credential())
            .isEqualTo(expectedCredentials.get(i).credential().serialize());
      }
    });
  }

  public enum BadCredentialsType {MISSING_START, MISSING_END, MISSING_BOTH}

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
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    when(backupManager.backupInfo(any())).thenReturn(CompletableFuture.completedFuture(new BackupManager.BackupInfo(
        1, "myBackupDir", "myMediaDir", "filename", Optional.empty())));
    final ArchiveController.BackupInfoResponse response = resources.getJerseyTest()
        .target("v1/archives")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ArchiveController.BackupInfoResponse.class);
    assertThat(response.backupDir()).isEqualTo("myBackupDir");
    assertThat(response.backupName()).isEqualTo("filename");
    assertThat(response.cdn()).isEqualTo(1);
    assertThat(response.usedSpace()).isEqualTo(0L);
  }

  @Test
  public void putMediaBatchSuccess() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    final byte[][] mediaIds = new byte[][]{TestRandomUtil.nextBytes(15), TestRandomUtil.nextBytes(15)};
    when(backupManager.copyToBackup(any(), any()))
        .thenReturn(Flux.just(
            new CopyResult(CopyResult.Outcome.SUCCESS, mediaIds[0], 1),
            new CopyResult(CopyResult.Outcome.SUCCESS, mediaIds[1], 1)));

    final Response r = resources.getJerseyTest()
        .target("v1/archives/media/batch")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaBatchRequest(List.of(
            new ArchiveController.CopyMediaRequest(
                new RemoteAttachment(3, "abc"),
                100,
                mediaIds[0],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32)),

            new ArchiveController.CopyMediaRequest(
                new RemoteAttachment(3, "def"),
                200,
                mediaIds[1],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32))
        ))));
    assertThat(r.getStatus()).isEqualTo(207);
    final ArchiveController.CopyMediaBatchResponse copyResponse = r.readEntity(
        ArchiveController.CopyMediaBatchResponse.class);
    assertThat(copyResponse.responses()).hasSize(2);
    for (int i = 0; i < 2; i++) {
      final ArchiveController.CopyMediaBatchResponse.Entry response = copyResponse.responses().get(i);
      assertThat(response.cdn()).isEqualTo(1);
      assertThat(response.mediaId()).isEqualTo(mediaIds[i]);
      assertThat(response.status()).isEqualTo(200);
    }
  }

  @Test
  public void putMediaBatchPartialFailure() throws VerificationFailedException {

    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));

    final byte[][] mediaIds = IntStream.range(0, 4).mapToObj(i -> TestRandomUtil.nextBytes(15)).toArray(byte[][]::new);
    when(backupManager.copyToBackup(any(), any()))
        .thenReturn(Flux.just(
            new CopyResult(CopyResult.Outcome.SUCCESS, mediaIds[0], 1),
            new CopyResult(CopyResult.Outcome.SOURCE_NOT_FOUND, mediaIds[1], null),
            new CopyResult(CopyResult.Outcome.SOURCE_WRONG_LENGTH, mediaIds[2], null),
            new CopyResult(CopyResult.Outcome.OUT_OF_QUOTA, mediaIds[3], null)));

    final List<ArchiveController.CopyMediaRequest> copyRequests = Arrays.stream(mediaIds)
        .map(mediaId -> new ArchiveController.CopyMediaRequest(
            new RemoteAttachment(3, "abc"),
            100,
            mediaId,
            TestRandomUtil.nextBytes(32),
            TestRandomUtil.nextBytes(32))
        ).toList();

    Response r = resources.getJerseyTest()
        .target("v1/archives/media/batch")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaBatchRequest(copyRequests)));
    assertThat(r.getStatus()).isEqualTo(207);
    final ArchiveController.CopyMediaBatchResponse copyResponse = r.readEntity(
        ArchiveController.CopyMediaBatchResponse.class);

    assertThat(copyResponse.responses()).hasSize(4);

    final ArchiveController.CopyMediaBatchResponse.Entry r1 = copyResponse.responses().getFirst();
    assertThat(r1.cdn()).isEqualTo(1);
    assertThat(r1.mediaId()).isEqualTo(mediaIds[0]);
    assertThat(r1.status()).isEqualTo(200);

    final ArchiveController.CopyMediaBatchResponse.Entry r2 = copyResponse.responses().get(1);
    assertThat(r2.mediaId()).isEqualTo(mediaIds[1]);
    assertThat(r2.status()).isEqualTo(410);
    assertThat(r2.failureReason()).isNotBlank();

    final ArchiveController.CopyMediaBatchResponse.Entry r3 = copyResponse.responses().get(2);
    assertThat(r3.mediaId()).isEqualTo(mediaIds[2]);
    assertThat(r3.status()).isEqualTo(400);
    assertThat(r3.failureReason()).isNotBlank();

    final ArchiveController.CopyMediaBatchResponse.Entry r4 = copyResponse.responses().get(3);
    assertThat(r4.mediaId()).isEqualTo(mediaIds[3]);
    assertThat(r4.status()).isEqualTo(413);
    assertThat(r4.failureReason()).isNotBlank();
  }


  @Test
  public void copyMediaWithNegativeLength() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    final byte[][] mediaIds = new byte[][]{TestRandomUtil.nextBytes(15), TestRandomUtil.nextBytes(15)};
    final Response r = resources.getJerseyTest()
        .target("v1/archives/media/batch")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaBatchRequest(List.of(
            new ArchiveController.CopyMediaRequest(
                new RemoteAttachment(3, "abc"),
                1,
                mediaIds[0],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32)),

            new ArchiveController.CopyMediaRequest(
                new RemoteAttachment(3, "def"),
                -1,
                mediaIds[1],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32))
        ))));
    assertThat(r.getStatus()).isEqualTo(422);
  }

  @CartesianTest
  public void list(
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorProvided,
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorReturned)
      throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));

    final byte[] mediaId = TestRandomUtil.nextBytes(15);
    final Optional<String> expectedCursor = cursorProvided ? Optional.of("myCursor") : Optional.empty();
    final Optional<String> returnedCursor = cursorReturned ? Optional.of("newCursor") : Optional.empty();

    when(backupManager.list(any(), eq(expectedCursor), eq(17)))
        .thenReturn(CompletableFuture.completedFuture(new BackupManager.ListMediaResult(
            List.of(new BackupManager.StorageDescriptorWithLength(1, mediaId, 100)),
            returnedCursor
        )));

    WebTarget target = resources.getJerseyTest()
        .target("v1/archives/media/")
        .queryParam("limit", 17);
    if (cursorProvided) {
      target = target.queryParam("cursor", "myCursor");
    }
    final ArchiveController.ListResponse response = target
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ArchiveController.ListResponse.class);

    assertThat(response.storedMediaObjects()).hasSize(1);
    assertThat(response.storedMediaObjects().getFirst().objectLength()).isEqualTo(100);
    assertThat(response.storedMediaObjects().getFirst().mediaId()).isEqualTo(mediaId);
    assertThat(response.cursor()).isEqualTo(returnedCursor.orElse(null));
  }

  @Test
  public void delete() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(BackupLevel.PAID,
        messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));

    final ArchiveController.DeleteMedia deleteRequest = new ArchiveController.DeleteMedia(
        IntStream
            .range(0, 100)
            .mapToObj(i -> new ArchiveController.DeleteMedia.MediaToDelete(3, TestRandomUtil.nextBytes(15)))
            .toList());

    when(backupManager.deleteMedia(any(), any()))
        .thenReturn(Flux.fromStream(deleteRequest.mediaToDelete().stream()
            .map(m -> new BackupManager.StorageDescriptor(m.cdn(), m.mediaId()))));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/media/delete")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .post(Entity.json(deleteRequest));
    assertThat(response.getStatus()).isEqualTo(204);
  }


  static Stream<Arguments> messagesUploadForm() {
    return Stream.of(
        Arguments.of(Optional.empty(), true),
        Arguments.of(Optional.of(BackupManager.MAX_MESSAGE_BACKUP_OBJECT_SIZE), true),
        Arguments.of(Optional.of(BackupManager.MAX_MESSAGE_BACKUP_OBJECT_SIZE + 1), false)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void messagesUploadForm(Optional<Long> uploadLength, boolean expectSuccess) throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    when(backupManager.createMessageBackupUploadDescriptor(any()))
        .thenReturn(CompletableFuture.completedFuture(
            new BackupUploadDescriptor(3, "abc", Map.of("k", "v"), "example.org")));

    final WebTarget builder = resources.getJerseyTest().target("v1/archives/upload/form");
    final Response response = uploadLength
        .map(length -> builder.queryParam("uploadLength", length))
        .orElse(builder)
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get();
    if (expectSuccess) {
      assertThat(response.getStatus()).isEqualTo(200);
      ArchiveController.UploadDescriptorResponse desc = response.readEntity(ArchiveController.UploadDescriptorResponse.class);
      assertThat(desc.cdn()).isEqualTo(3);
      assertThat(desc.key()).isEqualTo("abc");
      assertThat(desc.headers()).containsExactlyEntriesOf(Map.of("k", "v"));
      assertThat(desc.signedUploadLocation()).isEqualTo("example.org");
    } else {
      assertThat(response.getStatus()).isEqualTo(413);
    }
  }

  @Test
  public void mediaUploadForm() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    when(backupManager.createTemporaryAttachmentUploadDescriptor(any()))
        .thenReturn(CompletableFuture.completedFuture(
            new BackupUploadDescriptor(3, "abc", Map.of("k", "v"), "example.org")));
    final ArchiveController.UploadDescriptorResponse desc = resources.getJerseyTest()
        .target("v1/archives/media/upload/form")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ArchiveController.UploadDescriptorResponse.class);
    assertThat(desc.cdn()).isEqualTo(3);
    assertThat(desc.key()).isEqualTo("abc");
    assertThat(desc.headers()).containsExactlyEntriesOf(Map.of("k", "v"));
    assertThat(desc.signedUploadLocation()).isEqualTo("example.org");

    // rate limit
    when(backupManager.createTemporaryAttachmentUploadDescriptor(any()))
        .thenReturn(CompletableFuture.failedFuture(new RateLimitExceededException(null)));
    final Response response = resources.getJerseyTest()
        .target("v1/archives/media/upload/form")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get();
    assertThat(response.getStatus()).isEqualTo(429);
  }

  @Test
  public void readAuth() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    when(backupManager.generateReadAuth(any(), eq(3))).thenReturn(Map.of("key", "value"));
    final ArchiveController.ReadAuthResponse response = resources.getJerseyTest()
        .target("v1/archives/auth/read")
        .queryParam("cdn", 3)
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ArchiveController.ReadAuthResponse.class);
    assertThat(response.headers()).containsExactlyEntriesOf(Map.of("key", "value"));
  }


  @Test
  public void svrbAuth() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    final ExternalServiceCredentials credentials = new ExternalServiceCredentials("username", "password");
    when(backupManager.generateSvrbAuth(any())).thenReturn(credentials);
    final ExternalServiceCredentials response = resources.getJerseyTest()
        .target("v1/archives/auth/svrb")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get(ExternalServiceCredentials.class);
    assertThat(response).isEqualTo(credentials);
  }

  @Test
  public void readAuthInvalidParam() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    Response response = resources.getJerseyTest()
        .target("v1/archives/auth/read")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get();
    assertThat(response.getStatus()).isEqualTo(400);

    response = resources.getJerseyTest()
        .target("v1/archives/auth/read")
        .queryParam("abc")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .get();
    assertThat(response.getStatus()).isEqualTo(400);
  }

  @Test
  public void deleteEntireBackup() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    when(backupManager.deleteEntireBackup(any())).thenReturn(CompletableFuture.completedFuture(null));
    Response response = resources.getJerseyTest()
        .target("v1/archives/")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .delete();
    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  public void invalidSourceAttachmentKey() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.PAID, messagesBackupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupCredentialType.MESSAGES, BackupLevel.PAID)));
    final Response r = resources.getJerseyTest()
        .target("v1/archives/media")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaRequest(
            new RemoteAttachment(3, "invalid/urlBase64"),
            100,
            TestRandomUtil.nextBytes(15),
            TestRandomUtil.nextBytes(32),
            TestRandomUtil.nextBytes(32))));
    assertThat(r.getStatus()).isEqualTo(422);
  }

  private static AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupCredentialType credentialType, final BackupLevel backupLevel) {
    return new AuthenticatedBackupUser(backupId, credentialType, backupLevel, "myBackupDir", "myMediaDir", null);
  }
}
