/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.signal.chat.credentials.ExternalServiceType;
import org.signal.chat.credentials.GetCreateCallLinkCredentialsRequest;
import org.signal.chat.credentials.GetCreateCallLinkCredentialsResponse;
import org.signal.chat.credentials.GetDeliveryCertificateRequest;
import org.signal.chat.credentials.GetDeliveryCertificateResponse;
import org.signal.chat.credentials.GetExternalServiceCredentialsRequest;
import org.signal.chat.credentials.GetExternalServiceCredentialsResponse;
import org.signal.chat.credentials.GetGroupCredentialsRequest;
import org.signal.chat.credentials.GetGroupCredentialsResponse;
import org.signal.chat.credentials.SimpleCredentialsGrpc;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.auth.ServerZkAuthOperations;
import org.signal.libsignal.zkgroup.calllinks.CallLinkAuthCredentialResponse;
import org.signal.libsignal.zkgroup.calllinks.CreateCallLinkCredentialRequest;
import org.whispersystems.textsecuregcm.auth.CertificateGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.RedemptionRange;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class CredentialsGrpcService extends SimpleCredentialsGrpc.CredentialsImplBase {

  private final AccountsManager accountsManager;
  private final CertificateGenerator certificateGenerator;
  private final ServerZkAuthOperations serverZkAuthOperations;
  private final GenericServerSecretParams serverSecretParams;
  private final RateLimiters rateLimiters;
  private final Clock clock;

  private final Map<ExternalServiceType, ExternalServiceCredentialsGenerator> credentialsGeneratorByType;

  public CredentialsGrpcService(final AccountsManager accountsManager,
      final CertificateGenerator certificateGenerator,
      final ServerZkAuthOperations serverZkAuthOperations,
      final GenericServerSecretParams serverSecretParams,
      final RateLimiters rateLimiters,
      final Clock clock,
      final Map<ExternalServiceType, ExternalServiceCredentialsGenerator> credentialsGeneratorByType) {

    this.accountsManager = accountsManager;
    this.certificateGenerator = certificateGenerator;
    this.serverZkAuthOperations = serverZkAuthOperations;
    this.serverSecretParams = serverSecretParams;
    this.rateLimiters = rateLimiters;
    this.clock = clock;
    this.credentialsGeneratorByType = credentialsGeneratorByType;
  }

  @Override
  public GetExternalServiceCredentialsResponse getExternalServiceCredentials(final GetExternalServiceCredentialsRequest request)
      throws RateLimitExceededException {
    final ExternalServiceCredentialsGenerator credentialsGenerator = this.credentialsGeneratorByType
        .get(request.getExternalService());
    if (credentialsGenerator == null) {
      throw GrpcExceptions.fieldViolation("externalService", "Invalid external service type");
    }
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    rateLimiters.forDescriptor(RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS).validate(authenticatedDevice.accountIdentifier());
    final ExternalServiceCredentials externalServiceCredentials = credentialsGenerator
        .generateForUuid(authenticatedDevice.accountIdentifier());
    return GetExternalServiceCredentialsResponse.newBuilder()
        .setUsername(externalServiceCredentials.username())
        .setPassword(externalServiceCredentials.password())
        .build();
  }

  @Override
  public GetDeliveryCertificateResponse getDeliveryCertificate(final GetDeliveryCertificateRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    final Account account = accountsManager.getByAccountIdentifier(authenticatedDevice.accountIdentifier())
        .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));

    return GetDeliveryCertificateResponse.newBuilder()
        .setCertificateWithE164(ByteString.copyFrom(
            certificateGenerator.createFor(account, authenticatedDevice.deviceId(), true)))
        .setCertificateWithoutE164(ByteString.copyFrom(
            certificateGenerator.createFor(account, authenticatedDevice.deviceId(), false)))
        .build();
  }

  @Override
  public GetGroupCredentialsResponse getGroupCredentials(final GetGroupCredentialsRequest request) {
    final RedemptionRange redemptionRange;

    try {
      redemptionRange = RedemptionRange.inclusive(clock,
          Instant.ofEpochSecond(request.getRedemptionStartSeconds()),
          Instant.ofEpochSecond(request.getRedemptionEndSeconds()));
    } catch (final IllegalArgumentException e) {
      throw GrpcExceptions.invalidArguments(e.getMessage());
    }

    final Account account =
        accountsManager.getByAccountIdentifier(AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier())
            .orElseThrow(() -> GrpcExceptions.invalidCredentials("invalid credentials"));

    final ServiceId.Aci aci = new ServiceId.Aci(account.getIdentifier(IdentityType.ACI));
    final ServiceId.Pni pni = new ServiceId.Pni(account.getIdentifier(IdentityType.PNI));

    final GetGroupCredentialsResponse.Builder responseBuilder = GetGroupCredentialsResponse.newBuilder()
        .setPni(UUIDUtil.toByteString(pni.getRawUUID()));

    for (final Instant redemption : redemptionRange) {
      responseBuilder.addGroupCredentials(GetGroupCredentialsResponse.CredentialAndRedemptionTime.newBuilder()
          .setRedemptionTimeSeconds(redemption.getEpochSecond())
          .setCredential(ByteString.copyFrom(
              serverZkAuthOperations.issueAuthCredentialWithPniZkc(aci, pni, redemption).serialize()))
          .build());

      responseBuilder.addCallLinkAuthCredentials(GetGroupCredentialsResponse.CredentialAndRedemptionTime.newBuilder()
          .setRedemptionTimeSeconds(redemption.getEpochSecond())
          .setCredential(ByteString.copyFrom(
              CallLinkAuthCredentialResponse.issueCredential(aci, redemption, serverSecretParams).serialize()))
          .build());
    }

    return responseBuilder.build();
  }

  @Override
  public GetCreateCallLinkCredentialsResponse getCreateCallLinkCredentials(final GetCreateCallLinkCredentialsRequest request)
      throws RateLimitExceededException {

    final UUID accountIdentifier = AuthenticationUtil.requireAuthenticatedDevice().accountIdentifier();
    rateLimiters.getCreateCallLinkLimiter().validate(accountIdentifier);

    final Instant truncatedDayTimestamp = clock.instant().truncatedTo(ChronoUnit.DAYS);

    try {
      final CreateCallLinkCredentialRequest createCallLinkCredentialRequest =
          new CreateCallLinkCredentialRequest(request.getCredentialRequest().toByteArray());

      return GetCreateCallLinkCredentialsResponse.newBuilder()
          .setRedemptionTimeSeconds(truncatedDayTimestamp.getEpochSecond())
          .setCredential(ByteString.copyFrom(createCallLinkCredentialRequest.issueCredential(
                  new ServiceId.Aci(accountIdentifier),
                  truncatedDayTimestamp,
                  serverSecretParams)
              .serialize()))
          .build();
    } catch (final InvalidInputException e) {
      throw GrpcExceptions.invalidArguments("Invalid 'create call link credential' request");
    }
  }
}
