/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.CopyParameters;
import org.whispersystems.textsecuregcm.backup.CopyResult;
import org.whispersystems.textsecuregcm.backup.MediaEncryptionParameters;
import org.whispersystems.textsecuregcm.entities.RemoteAttachment;
import org.whispersystems.textsecuregcm.metrics.BackupMetrics;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.BackupAuthCredentialAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Mono;

@Path("/v1/archives")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Archive")
public class ArchiveController {

  public final static String X_SIGNAL_ZK_AUTH = "X-Signal-ZK-Auth";
  public final static String X_SIGNAL_ZK_AUTH_SIGNATURE = "X-Signal-ZK-Auth-Signature";

  private final AccountsManager accountsManager;
  private final BackupAuthManager backupAuthManager;
  private final BackupManager backupManager;
  private final BackupMetrics backupMetrics;

  public ArchiveController(
      final AccountsManager accountsManager,
      final BackupAuthManager backupAuthManager,
      final BackupManager backupManager,
      final BackupMetrics backupMetrics) {

    this.accountsManager = accountsManager;
    this.backupAuthManager = backupAuthManager;
    this.backupManager = backupManager;
    this.backupMetrics = backupMetrics;
  }

  public record SetBackupIdRequest(
      @Schema(description = """
          A BackupAuthCredentialRequest containing a blinded encrypted backup-id, encoded in standard padded base64.
          This backup-id should be used for message backups only, and must have the message backup type set on the
          credential.
          """, implementation = String.class)
      @JsonDeserialize(using = BackupAuthCredentialAdapter.CredentialRequestDeserializer.class)
      @JsonSerialize(using = BackupAuthCredentialAdapter.CredentialRequestSerializer.class)
      @NotNull BackupAuthCredentialRequest messagesBackupAuthCredentialRequest,

      @Schema(description = """
          A BackupAuthCredentialRequest containing a blinded encrypted backup-id, encoded in standard padded base64.
          This backup-id should be used for media only, and must have the media type set on the credential.
          """, implementation = String.class)
      @JsonDeserialize(using = BackupAuthCredentialAdapter.CredentialRequestDeserializer.class)
      @JsonSerialize(using = BackupAuthCredentialAdapter.CredentialRequestSerializer.class)
      @NotNull BackupAuthCredentialRequest mediaBackupAuthCredentialRequest) {}


  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/backupid")
  @Operation(
      summary = "Set backup id",
      description = """
          Set a (blinded) backup-id for the account. Each account may have a single active backup-id that can be used
          to store and retrieve backups. Once the backup-id is set, BackupAuthCredentials can be generated
          using /v1/archives/auth.

          The blinded backup-id and the key-pair used to blind it should be derived from a recoverable secret.
          """)
  @ApiResponse(responseCode = "204", description = "The backup-id was set")
  @ApiResponse(responseCode = "400", description = "The provided backup auth credential request was invalid")
  @ApiResponse(responseCode = "403", description = "The device did not have permission to set the backup-id. Only the primary device can set the backup-id for an account")
  @ApiResponse(responseCode = "429", description = "Rate limited. Too many attempts to change the backup-id have been made")
  public CompletionStage<Response> setBackupId(
      @Auth final AuthenticatedDevice authenticatedDevice,
      @Valid @NotNull final SetBackupIdRequest setBackupIdRequest) throws RateLimitExceededException {

    return accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          final Device device = account.getDevice(authenticatedDevice.deviceId())
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          return backupAuthManager
              .commitBackupId(account, device, setBackupIdRequest.messagesBackupAuthCredentialRequest,
                  setBackupIdRequest.mediaBackupAuthCredentialRequest)
              .thenApply(Util.ASYNC_EMPTY_RESPONSE);
        });
  }

  public record RedeemBackupReceiptRequest(
      @Schema(description = "Presentation of a ZK receipt encoded in standard padded base64", implementation = String.class)
      @JsonDeserialize(using = Deserializer.class)
      @NotNull
      ReceiptCredentialPresentation receiptCredentialPresentation) {

    public static class Deserializer extends JsonDeserializer<ReceiptCredentialPresentation> {

      @Override
      public ReceiptCredentialPresentation deserialize(JsonParser jsonParser,
          DeserializationContext deserializationContext) throws IOException {
        try {
          return new ReceiptCredentialPresentation(Base64.getDecoder().decode(jsonParser.getValueAsString()));
        } catch (InvalidInputException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/redeem-receipt")
  @Operation(
      summary = "Redeem receipt",
      description = """
          Redeem a receipt acquired from /v1/subscription/{subscriberId}/receipt_credentials to mark the account as
          eligible for the paid backup tier.

          After successful redemption, subsequent requests to /v1/archive/auth will return credentials with the level on
          the provided receipt until the expiration time on the receipt.

          Accounts must have an existing backup credential request in order to redeem a receipt. This request will fail
          if the account has not already set a backup credential request via PUT `/v1/archives/backupid`.
          """)
  @ApiResponse(responseCode = "204", description = "The receipt was redeemed")
  @ApiResponse(responseCode = "400", description = "The provided presentation or receipt was invalid")
  @ApiResponse(responseCode = "409", description = "The target account does not have a backup-id commitment")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  public CompletionStage<Response> redeemReceipt(
      @Auth final AuthenticatedDevice authenticatedDevice,
      @Valid @NotNull final RedeemBackupReceiptRequest redeemBackupReceiptRequest) {

    return accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          return backupAuthManager.redeemReceipt(account, redeemBackupReceiptRequest.receiptCredentialPresentation())
              .thenApply(Util.ASYNC_EMPTY_RESPONSE);
        });
  }

  public record BackupAuthCredentialsResponse(
      @Schema(description = "A map of credential types to lists of BackupAuthCredentials and their validity periods")
      Map<CredentialType, List<BackupAuthCredential>> credentials) {

    public enum CredentialType {
      MESSAGES,
      MEDIA;

      @JsonValue
      public String toValue() {
        return this.name().toLowerCase(Locale.ROOT);
      }

      @JsonCreator
      public static CredentialType fromValue(String v) {
        return v == null ? null : CredentialType.valueOf(v.toUpperCase(Locale.ROOT));
      }

      @VisibleForTesting
      static CredentialType fromLibsignalType(BackupCredentialType backupCredentialType) {
        return switch (backupCredentialType) {
          case MESSAGES -> BackupAuthCredentialsResponse.CredentialType.MESSAGES;
          case MEDIA -> BackupAuthCredentialsResponse.CredentialType.MEDIA;
        };
      }
    }

    public record BackupAuthCredential(
        @Schema(description = "A BackupAuthCredential, encoded in standard padded base64")
        byte[] credential,
        @Schema(description = "The day on which this credential is valid. Seconds since epoch truncated to day boundary")
        long redemptionTime) {}
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/auth")
  @Operation(
      summary = "Fetch ZK credentials ",
      description = """
          After setting a blinded backup-id with PUT /v1/archives/, this fetches credentials that can be used to perform
          operations against that backup-id. Clients may (and should) request up to 7 days of credentials at a time.

          The redemptionStart and redemptionEnd seconds must be UTC day aligned, and must not span more than 7 days.

          Each credential contains a receipt level which indicates the backup level the credential is good for. If the
          account has paid backup access that expires at some point in the provided redemption window, credentials with
          redemption times after the expiration may be on a lower backup level.

          Clients must validate the receipt level on the credential matches a known receipt level before using it.
          """)
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BackupAuthCredentialsResponse.class)))
  @ApiResponse(responseCode = "400", description = "The start/end did not meet alignment/duration requirements")
  @ApiResponse(responseCode = "404", description = "Could not find an existing blinded backup id")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  public CompletionStage<BackupAuthCredentialsResponse> getBackupZKCredentials(
      @Auth AuthenticatedDevice authenticatedDevice,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,
      @NotNull @QueryParam("redemptionStartSeconds") Long startSeconds,
      @NotNull @QueryParam("redemptionEndSeconds") Long endSeconds) {

    final Map<BackupCredentialType, List<BackupAuthCredentialsResponse.BackupAuthCredential>> credentialsByType =
        new ConcurrentHashMap<>();

    return accountsManager.getByAccountIdentifierAsync(authenticatedDevice.accountIdentifier())
        .thenCompose(maybeAccount -> {
          final Account account = maybeAccount
              .orElseThrow(() -> new WebApplicationException(Response.Status.UNAUTHORIZED));

          return CompletableFuture.allOf(Arrays.stream(BackupCredentialType.values())
                  .map(credentialType -> this.backupAuthManager.getBackupAuthCredentials(
                          account,
                          credentialType,
                          Instant.ofEpochSecond(startSeconds), Instant.ofEpochSecond(endSeconds))
                      .thenAccept(credentials -> {
                        backupMetrics.updateGetCredentialCounter(
                            UserAgentTagUtil.getPlatformTag(userAgent),
                            credentialType,
                            credentials.size());
                        credentialsByType.put(credentialType, credentials.stream()
                            .map(credential -> new BackupAuthCredentialsResponse.BackupAuthCredential(
                                credential.credential().serialize(),
                                credential.redemptionTime().getEpochSecond()))
                            .toList());
                      }))
                  .toArray(CompletableFuture[]::new))
              .thenApply(ignored -> new BackupAuthCredentialsResponse(credentialsByType.entrySet().stream()
                  .collect(Collectors.toMap(
                      e -> BackupAuthCredentialsResponse.CredentialType.fromLibsignalType(e.getKey()),
                      Map.Entry::getValue))));
        });
  }


  /**
   * API annotation for endpoints that take anonymous auth. All anonymous endpoints
   * <li> 400 if regular auth is used by accident </li>
   * <li> 401 if the anonymous auth invalid </li>
   * <li> 403 if the anonymous credential does not have sufficient permissions </li>
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @ApiResponse(
      responseCode = "403",
      description = "Forbidden. The request had insufficient permissions to perform the requested action")
  @ApiResponse(responseCode = "401", description = """
      The provided backup auth credential presentation could not be verified or
      The public key signature was invalid or
      There is no backup associated with the backup-id in the presentation or
      The credential was of the wrong type (messages/media)""")
  @ApiResponse(responseCode = "400", description = "Bad arguments. The request may have been made on an authenticated channel")
  @interface ApiResponseZkAuth {}

  public record BackupAuthCredentialPresentationHeader(BackupAuthCredentialPresentation presentation) {

    private static final String DESCRIPTION = "Presentation of a ZK backup auth credential acquired from /v1/archives/auth, encoded in standard padded base64";

    public BackupAuthCredentialPresentationHeader(final String header) {
      this(deserialize(header));
    }

    private static BackupAuthCredentialPresentation deserialize(final String base64Presentation) {
      byte[] bytes = Base64.getDecoder().decode(base64Presentation);
      try {
        return new BackupAuthCredentialPresentation(bytes);
      } catch (InvalidInputException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  public record BackupAuthCredentialPresentationSignature(byte[] signature) {

    private static final String DESCRIPTION = "Signature of the ZK auth credential's presentation, encoded in standard padded base64";

    public BackupAuthCredentialPresentationSignature(final String header) {
      this(Base64.getDecoder().decode(header));
    }
  }

  public record ReadAuthResponse(
      @Schema(description = "Auth headers to include with cdn read requests") Map<String, String> headers) {}

  @GET
  @Path("/auth/read")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Get CDN read credentials",
      description = "Retrieve credentials used to read objects stored on the backup cdn")
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ReadAuthResponse.class)))
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<ReadAuthResponse> readAuth(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @NotNull @Parameter(description = "The number of the CDN to get credentials for") @QueryParam("cdn") final Integer cdn) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenApply(user -> backupManager.generateReadAuth(user, cdn))
        .thenApply(ReadAuthResponse::new);
  }

  public record BackupInfoResponse(
      @Schema(description = "The CDN type where the message backup is stored. Media may be stored elsewhere.")
      int cdn,

      @Schema(description = """
          The base directory of your backup data on the cdn. The message backup can be found in the returned cdn at
          /backupDir/backupName and stored media can be found at /backupDir/mediaDir/mediaId
          """)
      String backupDir,

      @Schema(description = """
          The prefix path component for media objects on a cdn. Stored media for mediaId can be found at
          /backupDir/mediaDir/mediaId.
          """)
      String mediaDir,

      @Schema(description = "The name of the most recent message backup on the cdn. The backup is at /backupDir/backupName")
      String backupName,

      @Schema(description = "The amount of space used to store media")
      long usedSpace) {}

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Fetch backup info",
      description = "Retrieve information about the currently stored backup")
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BackupInfoResponse.class)))
  @ApiResponse(responseCode = "404", description = "No existing backups found")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<BackupInfoResponse> backupInfo(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }

    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupManager::backupInfo)
        .thenApply(backupInfo -> new BackupInfoResponse(
            backupInfo.cdn(),
            backupInfo.backupSubdir(),
            backupInfo.mediaSubdir(),
            backupInfo.messageBackupKey(),
            backupInfo.mediaUsedSpace().orElse(0L)));
  }

  public record SetPublicKeyRequest(
      @JsonSerialize(using = ECPublicKeyAdapter.Serializer.class)
      @JsonDeserialize(using = ECPublicKeyAdapter.Deserializer.class)
      @NotNull
      @Schema(type = "string", description = "The public key, serialized in libsignal's elliptic-curve public key format, and encoded in standard padded base64.")
      ECPublicKey backupIdPublicKey) {}

  @PUT
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Set public key",
      description = """
          Permanently set the public key of an ED25519 key-pair for the backup-id. All requests that provide a anonymous
          BackupAuthCredentialPresentation (including this one!) must also sign the presentation with the private key 
          corresponding to the provided public key.
          """)
  @ApiResponseZkAuth
  @ApiResponse(responseCode = "204", description = "The public key was set")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  public CompletionStage<Response> setPublicKey(
      @Auth final Optional<AuthenticatedDevice> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @Valid @NotNull SetPublicKeyRequest setPublicKeyRequest) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager
        .setPublicKey(presentation.presentation, signature.signature, setPublicKeyRequest.backupIdPublicKey)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }


  public record UploadDescriptorResponse(
      @Schema(description = "Indicates the CDN type. 3 indicates resumable uploads using TUS")
      int cdn,
      @Schema(description = "The location within the specified cdn where the finished upload can be found.")
      String key,
      @Schema(description = "A map of headers to include with all upload requests. Potentially contains time-limited upload credentials")
      Map<String, String> headers,
      @Schema(description = "The URL to upload to with the appropriate protocol")
      String signedUploadLocation) {}

  @GET
  @Path("/upload/form")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Fetch message backup upload form",
      description = "Retrieve an upload form that can be used to perform a resumable upload of a message backup.")
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UploadDescriptorResponse.class)))
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<UploadDescriptorResponse> backup(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupManager::createMessageBackupUploadDescriptor)
        .thenApply(result -> new UploadDescriptorResponse(
            result.cdn(),
            result.key(),
            result.headers(),
            result.signedUploadLocation()));
  }

  @GET
  @Path("/media/upload/form")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Fetch media attachment upload form",
      description = """
          Retrieve an upload form that can be used to perform a resumable upload of an attachment. After uploading, the
          attachment can be copied into the backup at PUT /archives/media/.

          Like the account authenticated version at /attachments, the uploaded object is only temporary.
          """)
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UploadDescriptorResponse.class)))
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<UploadDescriptorResponse> uploadTemporaryAttachment(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,


      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupManager::createTemporaryAttachmentUploadDescriptor)
        .thenApply(result -> new UploadDescriptorResponse(
            result.cdn(),
            result.key(),
            result.headers(),
            result.signedUploadLocation()));
  }

  public record CopyMediaRequest(
      @Schema(description = "The object on the attachment CDN to copy")
      @NotNull
      @Valid
      RemoteAttachment sourceAttachment,

      @Schema(description = "The length of the source attachment before the encryption applied by the copy operation")
      @NotNull
      @PositiveOrZero
      int objectLength,

      @Schema(description = "mediaId to copy on to the backup CDN, encoded in URL-safe padded base64", implementation = String.class)
      @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(15)
      byte[] mediaId,

      @Schema(description = "A 32-byte key for the MAC, encoded in standard padded base64", implementation = String.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(32)
      byte[] hmacKey,

      @Schema(description = "A 32-byte encryption key for AES, encoded in standard padded base64", implementation = String.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(32)
      byte[] encryptionKey) {

    CopyParameters toCopyParameters() {
      return new CopyParameters(
          sourceAttachment.cdn(), sourceAttachment.key(),
          objectLength,
          new MediaEncryptionParameters(encryptionKey, hmacKey),
          mediaId);
    }
  }

  public record CopyMediaResponse(
      @Schema(description = "The backup cdn where this media object is stored")
      @NotNull
      Integer cdn) {}

  @PUT
  @Path("/media/")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Backup media",
      description = """
          Copy and re-encrypt media from the attachments cdn into the backup cdn.

          The original, already encrypted, attachment will be encrypted with the provided key material before being copied.

          A particular destination media id should not be reused with a different source media id or different encryption
          parameters.
          """)
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = CopyMediaResponse.class)))
  @ApiResponse(responseCode = "400", description = "The provided object length was incorrect")
  @ApiResponse(responseCode = "413", description = "All media capacity has been consumed. Free some space to continue.")
  @ApiResponse(responseCode = "410", description = "The source object was not found.")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<CopyMediaResponse> copyMedia(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @NotNull
      @Valid final ArchiveController.CopyMediaRequest copyMediaRequest) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }

    return Mono
        .fromFuture(backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent))
        .flatMap(backupUser -> backupManager.copyToBackup(backupUser, List.of(copyMediaRequest.toCopyParameters()))
            .next()
            .doOnNext(result -> backupMetrics.updateCopyCounter(result, UserAgentTagUtil.getPlatformTag(userAgent)))
            .map(copyResult -> switch (copyResult.outcome()) {
              case SUCCESS -> new CopyMediaResponse(copyResult.cdn());
              case SOURCE_WRONG_LENGTH -> throw new BadRequestException("Invalid length");
              case SOURCE_NOT_FOUND -> throw new ClientErrorException("Source object not found", Response.Status.GONE);
              case OUT_OF_QUOTA ->
                  throw new ClientErrorException("Media quota exhausted", Response.Status.REQUEST_ENTITY_TOO_LARGE);
            }))
        .toFuture();
  }

  public record CopyMediaBatchRequest(
      @Schema(description = "A list of media objects to copy from the attachments CDN to the backup CDN")
      @NotNull
      @Size(min = 1, max = 1000)
      List<@Valid CopyMediaRequest> items) {}

  public record CopyMediaBatchResponse(

      @Schema(description = "Detailed outcome information for each copy request in the batch")
      List<Entry> responses) {

    public record Entry(
        @Schema(description = """
            The outcome of the copy attempt.
            A 200 indicates the object was successfully copied.
            A 400 indicates an invalid argument in the request
            A 410 indicates that the source object was not found
            A 413 indicates that the media quota was exhausted
            """)
        int status,

        @Schema(description = "On a copy failure, a detailed failure reason")
        String failureReason,

        @Schema(description = "The backup cdn where this media object is stored")
        Integer cdn,

        @Schema(description = "The mediaId of the object, encoded in URL-safe padded base64", implementation = String.class)
        @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
        @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
        @NotNull
        @ExactlySize(15)
        byte[] mediaId) {

      static Entry fromCopyResult(final CopyResult copyResult) {
        return switch (copyResult.outcome()) {
          case SUCCESS -> new Entry(200, null, copyResult.cdn(), copyResult.mediaId());
          case SOURCE_WRONG_LENGTH -> new Entry(400, "Invalid source length", null, copyResult.mediaId());
          case SOURCE_NOT_FOUND -> new Entry(410, "Source not found", null, copyResult.mediaId());
          case OUT_OF_QUOTA -> new Entry(413, "Media quota exhausted", null, copyResult.mediaId());
        };
      }
    }
  }

  @PUT
  @Path("/media/batch")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Batched backup media",
      description = """
          Copy and re-encrypt media from the attachments cdn into the backup cdn.

          The original already encrypted attachment will be encrypted with the provided key material before being copied

          If the batch request is processed at all, a 207 will be returned and the outcome of each constituent copy will
          be provided as a separate entry in the response.
          """)
  @ApiResponse(responseCode = "207", description = """
      The request was processed and each operation's outcome must be inspected individually. This does NOT necessarily 
      indicate the operation was a success.
      """, content = @Content(schema = @Schema(implementation = CopyMediaBatchResponse.class)))
  @ApiResponse(responseCode = "413", description = "All media capacity has been consumed. Free some space to continue.")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<Response> copyMedia(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @NotNull
      @Valid final ArchiveController.CopyMediaBatchRequest copyMediaRequest) {

    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    final Stream<CopyParameters> copyParams = copyMediaRequest.items().stream().map(CopyMediaRequest::toCopyParameters);
    return Mono.fromFuture(backupManager.authenticateBackupUser(presentation.presentation, signature.signature, userAgent))
        .flatMapMany(backupUser -> backupManager.copyToBackup(backupUser, copyParams.toList()))
        .doOnNext(result -> backupMetrics.updateCopyCounter(result, UserAgentTagUtil.getPlatformTag(userAgent)))
        .map(CopyMediaBatchResponse.Entry::fromCopyResult)
        .collectList()
        .map(list -> Response.status(207).entity(new CopyMediaBatchResponse(list)).build())
        .toFuture();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(
      summary = "Refresh backup",
      description = """
          Indicate that this backup is still active. Clients must periodically upload new backups or perform a refresh
          via a POST request. If a backup is not refreshed, after 30 days it may be deleted.
          """)
  @ApiResponse(responseCode = "204", description = "The backup was successfully refreshed")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<Response> refresh(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager
        .authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupManager::ttlRefresh)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  record StoredMediaObject(

      @Schema(description = "The backup cdn where this media object is stored")
      @NotNull
      Integer cdn,

      @Schema(description = "The mediaId of the object in URL-safe base64", implementation = String.class)
      @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
      @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(15)
      byte[] mediaId,

      @Schema(description = "The length of the object in bytes")
      @NotNull
      Long objectLength) {}

  public record ListResponse(
      @Schema(description = "A page of media objects stored for this backup ID")
      List<StoredMediaObject> storedMediaObjects,

      @Schema(description = """
          The base directory of your backup data on the cdn. The stored media can be found at /backupDir/mediaDir/mediaId
          """)
      String backupDir,

      @Schema(description = """
          The prefix path component for the media objects. The stored media for mediaId can be found at /backupDir/mediaDir/mediaId.
          """)
      String mediaDir,
      @Schema(description = "If set, the cursor value to pass to the next list request to continue listing. If absent, all objects have been listed")
      String cursor) {}

  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/media")
  @Operation(summary = "List media objects",
      description = """
          Retrieve a list of media objects stored for this backup-id. A client may have previously stored media objects
          that are no longer referenced in their current backup. To reclaim storage space used by these orphaned
          objects, perform a list operation and remove any unreferenced media objects via DELETE /v1/backups/<mediaId>.
          """)
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ListResponse.class)))
  @ApiResponse(responseCode = "400", description = "Invalid cursor or limit")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<ListResponse> listMedia(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @Parameter(description = "A cursor returned by a previous call")
      @QueryParam("cursor") final Optional<String> cursor,

      @Parameter(description = "The number of entries to return per call")
      @QueryParam("limit") final Optional<@Min(1) @Max(10_000) Integer> limit) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager
        .authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupUser -> backupManager.list(backupUser, cursor, limit.orElse(1000))
            .thenApply(result -> new ListResponse(
                result.media()
                    .stream().map(entry -> new StoredMediaObject(entry.cdn(), entry.key(), entry.length()))
                    .toList(),
                backupUser.backupDir(),
                backupUser.mediaDir(),
                result.cursor().orElse(null))));
  }

  public record DeleteMedia(@Size(min = 1, max = 1000) List<@Valid MediaToDelete> mediaToDelete) {

    public record MediaToDelete(
        @Schema(description = "The backup cdn where this media object is stored")
        @NotNull
        Integer cdn,

        @Schema(description = "The mediaId of the object in URL-safe base64", implementation = String.class)
        @JsonSerialize(using = ByteArrayBase64UrlAdapter.Serializing.class)
        @JsonDeserialize(using = ByteArrayBase64UrlAdapter.Deserializing.class)
        @NotNull
        @ExactlySize(15)
        byte[] mediaId
    ) {}
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/media/delete")
  @Operation(summary = "Delete media objects",
      description = "Delete media objects stored with this backup-id")
  @ApiResponse(responseCode = "204", description = "The provided objects were successfully deleted or they do not exist")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<Response> deleteMedia(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @Valid @NotNull DeleteMedia deleteMedia) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }

    final List<BackupManager.StorageDescriptor> toDelete = deleteMedia.mediaToDelete().stream()
        .map(media -> new BackupManager.StorageDescriptor(media.cdn(), media.mediaId))
        .toList();

    return backupManager
        .authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(authenticatedBackupUser -> backupManager
            .deleteMedia(authenticatedBackupUser, toDelete)
            .then().toFuture())
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Delete entire backup", description = """
      Delete all backup metadata, objects, and stored public key. To use backups again, a public key must be resupplied.
      """)
  @ApiResponse(responseCode = "204", description = "The backup has been successfully removed")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<Response> deleteBackup(
      @Auth final Optional<AuthenticatedDevice> account,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgent,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager
        .authenticateBackupUser(presentation.presentation, signature.signature, userAgent)
        .thenCompose(backupManager::deleteEntireBackup)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

}
