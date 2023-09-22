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
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.backup.BackupAuthManager;
import org.whispersystems.textsecuregcm.backup.BackupManager;
import org.whispersystems.textsecuregcm.util.BackupAuthCredentialAdapter;
import org.whispersystems.textsecuregcm.util.ECPublicKeyAdapter;

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
          A BackupAuthCredentialRequest containing a blinded encrypted backup-id, encoded as a base64 string
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
  public CompletionStage<Void> setBackupId(
      @Auth final AuthenticatedAccount account,
      @Valid @NotNull final SetBackupIdRequest setBackupIdRequest) throws RateLimitExceededException {
    return this.backupAuthManager.commitBackupId(account.getAccount(), setBackupIdRequest.backupAuthCredentialRequest);
  }

  public record BackupAuthCredentialsResponse(
      @Schema(description = "A list of BackupAuthCredentials and their validity periods")
      List<BackupAuthCredential> credentials) {

    public record BackupAuthCredential(
        @Schema(description = "A base64 encoded BackupAuthCredential")
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
      @Auth AuthenticatedAccount auth,
      @NotNull @QueryParam("redemptionStartSeconds") Integer startSeconds,
      @NotNull @QueryParam("redemptionEndSeconds") Integer endSeconds) {

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

    private static final String DESCRIPTION = "Presentation of a ZK backup auth credential acquired from /v1/archives/auth as a base64 encoded string";

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

    private static final String DESCRIPTION = "Signature of the ZK auth credential's presentation as a base64 encoded string";

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
      @Auth final Optional<AuthenticatedAccount> account,

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

      @Schema(description = "If present, the directory of your backup data on the cdn.")
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
      @Auth final Optional<AuthenticatedAccount> account,

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
      @Schema(type = "string", description = "The public key, serialized in libsignal's elliptic-curve public key format and then base64-encoded.")
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
  public CompletionStage<Void> setPublicKey(
      @Auth final Optional<AuthenticatedAccount> account,

      @Parameter(description = BackupAuthCredentialPresentationHeader.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH) final ArchiveController.BackupAuthCredentialPresentationHeader presentation,

      @Parameter(description = BackupAuthCredentialPresentationSignature.DESCRIPTION, schema = @Schema(implementation = String.class))
      @NotNull
      @HeaderParam(X_SIGNAL_ZK_AUTH_SIGNATURE) final BackupAuthCredentialPresentationSignature signature,

      @NotNull SetPublicKeyRequest setPublicKeyRequest) {
    return backupManager.setPublicKey(
        presentation.presentation, signature.signature,
        setPublicKeyRequest.backupIdPublicKey);
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
      @Auth final Optional<AuthenticatedAccount> account,

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
  public CompletionStage<Void> refresh(
      @Auth final Optional<AuthenticatedAccount> account,

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
        .thenCompose(backupManager::ttlRefresh);
  }
}
