/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.backup.InvalidLengthException;
import org.whispersystems.textsecuregcm.backup.MediaEncryptionParameters;
import org.whispersystems.textsecuregcm.backup.SourceObjectNotFoundException;
import org.whispersystems.textsecuregcm.util.BackupAuthCredentialAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;
import org.whispersystems.textsecuregcm.util.ByteArrayBase64UrlAdapter;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import org.whispersystems.websocket.auth.Mutable;
import org.whispersystems.websocket.auth.ReadOnly;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("/v1/archives")
@Tag(name = "Archive")
public class ArchiveController {

  public final static String X_SIGNAL_ZK_AUTH = "X-Signal-ZK-Auth";
  public final static String X_SIGNAL_ZK_AUTH_SIGNATURE = "X-Signal-ZK-Auth-Signature";

  private final BackupAuthManager backupAuthManager;
  private final BackupManager backupManager;

  public ArchiveController(
      final BackupAuthManager backupAuthManager,
      final BackupManager backupManager) {
    this.backupAuthManager = backupAuthManager;
    this.backupManager = backupManager;
  }

  public record SetBackupIdRequest(
      @Schema(description = """
          A BackupAuthCredentialRequest containing a blinded encrypted backup-id, encoded in standard padded base64
          """, implementation = String.class)
      @JsonDeserialize(using = BackupAuthCredentialAdapter.CredentialRequestDeserializer.class)
      @JsonSerialize(using = BackupAuthCredentialAdapter.CredentialRequestSerializer.class)
      @NotNull BackupAuthCredentialRequest backupAuthCredentialRequest) {}


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
  @ApiResponse(responseCode = "429", description = "Rate limited. Too many attempts to change the backup-id have been made")
  public CompletionStage<Response> setBackupId(
      @Mutable @Auth final AuthenticatedAccount account,
      @Valid @NotNull final SetBackupIdRequest setBackupIdRequest) throws RateLimitExceededException {
    return this.backupAuthManager
        .commitBackupId(account.getAccount(), setBackupIdRequest.backupAuthCredentialRequest)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }

  public record BackupAuthCredentialsResponse(
      @Schema(description = "A list of BackupAuthCredentials and their validity periods")
      List<BackupAuthCredential> credentials) {

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
          """)
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BackupAuthCredentialsResponse.class)))
  @ApiResponse(responseCode = "400", description = "The start/end did not meet alignment/duration requirements")
  @ApiResponse(responseCode = "404", description = "Could not find an existing blinded backup id")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  public CompletionStage<BackupAuthCredentialsResponse> getBackupZKCredentials(
      @ReadOnly @Auth AuthenticatedAccount auth,
      @NotNull @QueryParam("redemptionStartSeconds") Long startSeconds,
      @NotNull @QueryParam("redemptionEndSeconds") Long endSeconds) {

    return this.backupAuthManager.getBackupAuthCredentials(
            auth.getAccount(),
            Instant.ofEpochSecond(startSeconds), Instant.ofEpochSecond(endSeconds))
        .thenApply(creds -> new BackupAuthCredentialsResponse(creds.stream()
            .map(cred -> new BackupAuthCredentialsResponse.BackupAuthCredential(
                cred.credential().serialize(),
                cred.redemptionTime().getEpochSecond()))
            .toList()));
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
  @ApiResponse(responseCode = "401", description = "The provided backup auth credential presentation could not be verified")
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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature)
        .thenApply(backupManager::generateReadAuth)
        .thenApply(ReadAuthResponse::new);
  }

  public record BackupInfoResponse(
      @Schema(description = "If present, the CDN type where the message backup is stored")
      int cdn,

      @Schema(description = """
          If present, the directory of your backup data on the cdn. The message backup can be found at /backupDir/backupName
          and stored media can be found at /backupDir/media/mediaId.
          """)
      String backupDir,

      @Schema(description = "If present, the name of the most recent message backup on the cdn. The backup is at /backupDir/backupName")
      String backupName,

      @Nullable
      @Schema(description = "The amount of space used to store media")
      Long usedSpace) {}

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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }

    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature)
        .thenCompose(backupManager::backupInfo)
        .thenApply(backupInfo -> new BackupInfoResponse(
            backupInfo.cdn(),
            backupInfo.backupSubdir(),
            backupInfo.messageBackupKey(),
            backupInfo.mediaUsedSpace().orElse(null)));
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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @Valid @NotNull SetPublicKeyRequest setPublicKeyRequest) {
    return backupManager
        .setPublicKey(presentation.presentation, signature.signature, setPublicKeyRequest.backupIdPublicKey)
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }


  public record MessageBackupResponse(
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
  @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = MessageBackupResponse.class)))
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<MessageBackupResponse> backup(
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature) {
    if (account.isPresent()) {
      throw new BadRequestException("must not use authenticated connection for anonymous operations");
    }
    return backupManager.authenticateBackupUser(presentation.presentation, signature.signature)
        .thenCompose(backupManager::createMessageBackupUploadDescriptor)
        .thenApply(result -> new MessageBackupResponse(
            result.cdn(),
            result.key(),
            result.headers(),
            result.signedUploadLocation()));
  }

  public record RemoteAttachment(
      @Schema(description = "The attachment cdn")
      @NotNull
      Integer cdn,
      @NotBlank
      @Schema(description = "The attachment key")
      String key) {}

  public record CopyMediaRequest(
      @Schema(description = "The object on the attachment CDN to copy")
      @NotNull
      RemoteAttachment sourceAttachment,

      @Schema(description = "The length of the source attachment before the encryption applied by the copy operation")
      @NotNull
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
      byte[] encryptionKey,

      @Schema(description = "A 16-byte IV for AES, encoded in standard padded base64", implementation = String.class)
      @JsonDeserialize(using = ByteArrayAdapter.Deserializing.class)
      @NotNull
      @ExactlySize(16)
      byte[] iv) {}

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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

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

    final AuthenticatedBackupUser backupUser = backupManager.authenticateBackupUser(
        presentation.presentation, signature.signature).join();

    final boolean fits = backupManager.canStoreMedia(backupUser, copyMediaRequest.objectLength()).join();
    if (!fits) {
      throw new ClientErrorException("Media quota exhausted", Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }
    return copyMediaImpl(backupUser, copyMediaRequest)
        .thenApply(result -> new CopyMediaResponse(result.cdn()))
        .exceptionally(e -> {
          final Throwable unwrapped = ExceptionUtils.unwrap(e);
          if (unwrapped instanceof SourceObjectNotFoundException) {
            throw new ClientErrorException("Source object not found " + unwrapped.getMessage(), Response.Status.GONE);
          } else if (unwrapped instanceof InvalidLengthException) {
            throw new BadRequestException("Invalid length " + unwrapped.getMessage());
          } else {
            throw ExceptionUtils.wrap(e);
          }
        });
  }

  private CompletionStage<BackupManager.StorageDescriptor> copyMediaImpl(final AuthenticatedBackupUser backupUser,
      final CopyMediaRequest copyMediaRequest) {
    return this.backupManager.copyToBackup(
        backupUser,
        copyMediaRequest.sourceAttachment.cdn,
        copyMediaRequest.sourceAttachment.key,
        copyMediaRequest.objectLength,
        new MediaEncryptionParameters(
            copyMediaRequest.encryptionKey,
            copyMediaRequest.hmacKey,
            copyMediaRequest.iv),
        copyMediaRequest.mediaId);
  }


  public record CopyMediaBatchRequest(
      @Schema(description = "A list of media objects to copy from the attachments CDN to the backup CDN")
      @NotNull
      @Size(min = 1, max = 1000)
      List<CopyMediaRequest> items) {}

  public record CopyMediaBatchResponse(

      @Schema(description = "Detailed outcome information for each copy request in the batch")
      List<Entry> responses) {

    public record Entry(
        @Schema(description = """
            The outcome of the copy attempt.
            A 200 indicates the object was successfully copied.
            A 400 indicates an invalid argument in the request
            A 410 indicates that the source object was not found
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
        byte[] mediaId) {}
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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

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

    final AuthenticatedBackupUser backupUser = backupManager.authenticateBackupUser(
        presentation.presentation, signature.signature).join();

    // If the entire batch won't fit in the user's remaining quota, reject the whole request.
    final long expectedStorage = copyMediaRequest.items().stream().mapToLong(CopyMediaRequest::objectLength).sum();
    final boolean fits = backupManager.canStoreMedia(backupUser, expectedStorage).join();
    if (!fits) {
      throw new ClientErrorException("Media quota exhausted", Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }

    return Flux.fromIterable(copyMediaRequest.items)
        // Operate sequentially, waiting for one copy to finish before starting the next one. At least right now,
        // copying concurrently will introduce contention over the metadata.
        .concatMap(request -> Mono
            .fromCompletionStage(copyMediaImpl(backupUser, request))
            .map(result -> new CopyMediaBatchResponse.Entry(200, null, result.cdn(), result.key()))
            .onErrorResume(throwable -> ExceptionUtils.unwrap(throwable) instanceof IOException, throwable -> {
              final Throwable unwrapped = ExceptionUtils.unwrap(throwable);

              int status;
              String error;
              if (unwrapped instanceof SourceObjectNotFoundException) {
                status = 410;
                error = "Source object not found " + unwrapped.getMessage();
              } else if (unwrapped instanceof InvalidLengthException) {
                status = 400;
                error = "Invalid length " + unwrapped.getMessage();
              } else {
                throw ExceptionUtils.wrap(throwable);
              }
              return Mono.just(new CopyMediaBatchResponse.Entry(status, error, null, request.mediaId));
            }))
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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

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
        .authenticateBackupUser(presentation.presentation, signature.signature)
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
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

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
        .authenticateBackupUser(presentation.presentation, signature.signature)
        .thenCompose(backupUser -> backupManager.list(backupUser, cursor, limit.orElse(1000)))
        .thenApply(result -> new ListResponse(
            result.media()
                .stream().map(entry -> new StoredMediaObject(entry.cdn(), entry.key(), entry.length()))
                .toList(),
            result.cursor().orElse(null)));
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
  @ApiResponse(responseCode = "204")
  @ApiResponse(responseCode = "429", description = "Rate limited.")
  @ApiResponseZkAuth
  public CompletionStage<Response> deleteMedia(
      @ReadOnly @Auth final Optional<AuthenticatedAccount> account,

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

    return backupManager
        .authenticateBackupUser(presentation.presentation, signature.signature)
        .thenCompose(authenticatedBackupUser -> backupManager.delete(authenticatedBackupUser,
            deleteMedia.mediaToDelete().stream()
                .map(media -> new BackupManager.StorageDescriptor(media.cdn(), media.mediaId))
                .toList()))
        .thenApply(Util.ASYNC_EMPTY_RESPONSE);
  }
}
