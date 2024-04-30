/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.grpc.Status;
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
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ClientZkReceiptOperations;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredential;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialRequestContext;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialResponse;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupAuthTestUtil;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.InvalidLengthException;
import org.whispersystems.textsecuregcm.backup.SourceObjectNotFoundException;
import org.whispersystems.textsecuregcm.backup.BackupUploadDescriptor;
import org.whispersystems.textsecuregcm.mappers.CompletionExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.GrpcStatusRuntimeExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
public class ArchiveControllerTest {

  private static final BackupAuthManager backupAuthManager = mock(BackupAuthManager.class);
  private static final BackupManager backupManager = mock(BackupManager.class);
  private final BackupAuthTestUtil backupAuthTestUtil = new BackupAuthTestUtil(Clock.systemUTC());

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedAccount.class))
      .addProvider(new CompletionExceptionMapper())
      .addResource(new GrpcStatusRuntimeExceptionMapper())
      .addProvider(new RateLimitExceededExceptionMapper())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new ArchiveController(backupAuthManager, backupManager))
      .build();

  private final UUID aci = UUID.randomUUID();
  private final byte[] backupKey = TestRandomUtil.nextBytes(32);

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
        BackupLevel.MEDIA, backupKey, aci);
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
        BackupLevel.MEDIA, backupKey, aci);
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
        BackupLevel.MEDIA, backupKey, aci);
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
        BackupLevel.MEDIA, backupKey, aci);
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
        BackupLevel.MEDIA, backupAuthTestUtil.getRequest(backupKey, aci), start, end);
    when(backupAuthManager.getBackupAuthCredentials(any(), eq(start), eq(end))).thenReturn(
        CompletableFuture.completedFuture(expectedResponse));
    final ArchiveController.BackupAuthCredentialsResponse creds = resources.getJerseyTest()
        .target("v1/archives/auth")
        .queryParam("redemptionStartSeconds", start.getEpochSecond())
        .queryParam("redemptionEndSeconds", end.getEpochSecond())
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get(ArchiveController.BackupAuthCredentialsResponse.class);
    assertThat(creds.credentials().getFirst().redemptionTime()).isEqualTo(start.getEpochSecond());
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
        BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));
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
        BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));
    when(backupManager.canStoreMedia(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(true));
    when(backupManager.copyToBackup(any(), anyInt(), any(), anyInt(), any(), any()))
        .thenAnswer(invocation -> {
          byte[] mediaId = invocation.getArgument(5, byte[].class);
          return CompletableFuture.completedFuture(new BackupManager.StorageDescriptor(1, mediaId));
        });

    final byte[][] mediaIds = new byte[][]{TestRandomUtil.nextBytes(15), TestRandomUtil.nextBytes(15)};

    final Response r = resources.getJerseyTest()
        .target("v1/archives/media/batch")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaBatchRequest(List.of(
            new ArchiveController.CopyMediaRequest(
                new ArchiveController.RemoteAttachment(3, "abc"),
                100,
                mediaIds[0],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(16)),

            new ArchiveController.CopyMediaRequest(
                new ArchiveController.RemoteAttachment(3, "def"),
                200,
                mediaIds[1],
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(16))
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
        BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));

    final byte[][] mediaIds = IntStream.range(0, 3).mapToObj(i -> TestRandomUtil.nextBytes(15)).toArray(byte[][]::new);
    when(backupManager.canStoreMedia(any(), anyLong())).thenReturn(CompletableFuture.completedFuture(true));

    when(backupManager.copyToBackup(any(), anyInt(), any(), anyInt(), any(), eq(mediaIds[0])))
        .thenReturn(CompletableFuture.completedFuture(new BackupManager.StorageDescriptor(1, mediaIds[0])));
    when(backupManager.copyToBackup(any(), anyInt(), any(), anyInt(), any(), eq(mediaIds[1])))
        .thenReturn(CompletableFuture.failedFuture(new SourceObjectNotFoundException()));
    when(backupManager.copyToBackup(any(), anyInt(), any(), anyInt(), any(), eq(mediaIds[2])))
        .thenReturn(CompletableFuture.failedFuture(new InvalidLengthException("bad length")));

    final List<ArchiveController.CopyMediaRequest> copyRequests = Arrays.stream(mediaIds)
        .map(mediaId -> new ArchiveController.CopyMediaRequest(
            new ArchiveController.RemoteAttachment(3, "abc"),
            100,
            mediaId,
            TestRandomUtil.nextBytes(32),
            TestRandomUtil.nextBytes(32),
            TestRandomUtil.nextBytes(16))
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

    assertThat(copyResponse.responses()).hasSize(3);

    final ArchiveController.CopyMediaBatchResponse.Entry r1 = copyResponse.responses().get(0);
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
  }

  @Test
  public void putMediaBatchOutOfSpace() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));

    when(backupManager.canStoreMedia(any(), eq(1L + 2L + 3L)))
        .thenReturn(CompletableFuture.completedFuture(false));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/media/batch")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .put(Entity.json(new ArchiveController.CopyMediaBatchRequest(IntStream.range(0, 3)
            .mapToObj(i -> new ArchiveController.CopyMediaRequest(
                new ArchiveController.RemoteAttachment(3, "abc"),
                i + 1,
                TestRandomUtil.nextBytes(15),
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(32),
                TestRandomUtil.nextBytes(16))
            ).toList())));
    assertThat(response.getStatus()).isEqualTo(413);
  }

  @CartesianTest
  public void list(
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorProvided,
      @CartesianTest.Values(booleans = {true, false}) final boolean cursorReturned)
      throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(
        BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));

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
    final BackupAuthCredentialPresentation presentation = backupAuthTestUtil.getPresentation(BackupLevel.MEDIA,
        backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));

    final ArchiveController.DeleteMedia deleteRequest = new ArchiveController.DeleteMedia(
        IntStream
            .range(0, 100)
            .mapToObj(i -> new ArchiveController.DeleteMedia.MediaToDelete(3, TestRandomUtil.nextBytes(15)))
            .toList());

    when(backupManager.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final Response response = resources.getJerseyTest()
        .target("v1/archives/media/delete")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .post(Entity.json(deleteRequest));
    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  public void mediaUploadForm() throws RateLimitExceededException, VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));
    when(backupManager.createTemporaryAttachmentUploadDescriptor(any()))
        .thenReturn(new BackupUploadDescriptor(3, "abc", Map.of("k", "v"), "example.org"));
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
        .thenThrow(new RateLimitExceededException(null, false));
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
        backupAuthTestUtil.getPresentation(BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));
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
  public void readAuthInvalidParam() throws VerificationFailedException {
    final BackupAuthCredentialPresentation presentation =
        backupAuthTestUtil.getPresentation(BackupLevel.MEDIA, backupKey, aci);
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
        backupAuthTestUtil.getPresentation(BackupLevel.MEDIA, backupKey, aci);
    when(backupManager.authenticateBackupUser(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(backupUser(presentation.getBackupId(), BackupLevel.MEDIA)));
    when(backupManager.deleteEntireBackup(any())).thenReturn(CompletableFuture.completedFuture(null));
    Response response = resources.getJerseyTest()
        .target("v1/archives/")
        .request()
        .header("X-Signal-ZK-Auth", Base64.getEncoder().encodeToString(presentation.serialize()))
        .header("X-Signal-ZK-Auth-Signature", "aaa")
        .delete();
    assertThat(response.getStatus()).isEqualTo(204);
  }

  private static AuthenticatedBackupUser backupUser(byte[] backupId, BackupLevel backupLevel) {
    return new AuthenticatedBackupUser(backupId, backupLevel, "myBackupDir", "myMediaDir");
  }
}
