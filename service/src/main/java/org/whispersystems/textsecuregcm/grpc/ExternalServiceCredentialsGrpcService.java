/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import java.time.Clock;
import java.util.Map;
import org.signal.chat.credentials.ExternalServiceType;
import org.signal.chat.credentials.GetExternalServiceCredentialsRequest;
import org.signal.chat.credentials.GetExternalServiceCredentialsResponse;
import org.signal.chat.credentials.ReactorExternalServiceCredentialsGrpc;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import reactor.core.publisher.Mono;

public class ExternalServiceCredentialsGrpcService extends ReactorExternalServiceCredentialsGrpc.ExternalServiceCredentialsImplBase {

  private final Map<ExternalServiceType, ExternalServiceCredentialsGenerator> credentialsGeneratorByType;

  private final RateLimiters rateLimiters;


  public static ExternalServiceCredentialsGrpcService createForAllExternalServices(
      final WhisperServerConfiguration chatConfiguration,
      final RateLimiters rateLimiters) {
    return new ExternalServiceCredentialsGrpcService(
        ExternalServiceDefinitions.createExternalServiceList(chatConfiguration, Clock.systemUTC()),
        rateLimiters
    );
  }

  @VisibleForTesting
  ExternalServiceCredentialsGrpcService(
      final Map<ExternalServiceType, ExternalServiceCredentialsGenerator> credentialsGeneratorByType,
      final RateLimiters rateLimiters) {
    this.credentialsGeneratorByType = requireNonNull(credentialsGeneratorByType);
    this.rateLimiters = requireNonNull(rateLimiters);
  }

  @Override
  public Mono<GetExternalServiceCredentialsResponse> getExternalServiceCredentials(final GetExternalServiceCredentialsRequest request) {
    final ExternalServiceCredentialsGenerator credentialsGenerator = this.credentialsGeneratorByType
        .get(request.getExternalService());
    if (credentialsGenerator == null) {
      return Mono.error(Status.INVALID_ARGUMENT.asException());
    }
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    return rateLimiters.forDescriptor(RateLimiters.For.EXTERNAL_SERVICE_CREDENTIALS).validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromSupplier(() -> {
          final ExternalServiceCredentials externalServiceCredentials = credentialsGenerator
              .generateForUuid(authenticatedDevice.accountIdentifier());
          return GetExternalServiceCredentialsResponse.newBuilder()
              .setUsername(externalServiceCredentials.username())
              .setPassword(externalServiceCredentials.password())
              .build();
        }));
  }
}
