/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.signal.chat.credentials.ExternalServiceType;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.DirectoryV2ClientConfiguration;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery2Configuration;

enum ExternalServiceDefinitions {
  DIRECTORY(ExternalServiceType.EXTERNAL_SERVICE_TYPE_DIRECTORY, (chatConfig, clock) -> {
    final DirectoryV2ClientConfiguration cfg = chatConfig.getDirectoryV2Configuration().getDirectoryV2ClientConfiguration();
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret())
        .prependUsername(false)
        .withClock(clock)
        .build();
  }),
  PAYMENTS(ExternalServiceType.EXTERNAL_SERVICE_TYPE_PAYMENTS, (chatConfig, clock) -> {
    final PaymentsServiceConfiguration cfg = chatConfig.getPaymentsServiceConfiguration();
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }),
  SVR(ExternalServiceType.EXTERNAL_SERVICE_TYPE_SVR, (chatConfig, clock) -> {
    final SecureValueRecovery2Configuration cfg = chatConfig.getSvr2Configuration();
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .withUserDerivationKey(cfg.userIdTokenSharedSecret().value())
        .prependUsername(false)
        .withDerivedUsernameTruncateLength(16)
        .withClock(clock)
        .build();
  }),
  STORAGE(ExternalServiceType.EXTERNAL_SERVICE_TYPE_STORAGE, (chatConfig, clock) -> {
    final PaymentsServiceConfiguration cfg = chatConfig.getPaymentsServiceConfiguration();
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }),
  ;

  private final ExternalServiceType externalService;

  private final BiFunction<WhisperServerConfiguration, Clock, ExternalServiceCredentialsGenerator> generatorFactory;

  ExternalServiceDefinitions(
      final ExternalServiceType externalService,
      final BiFunction<WhisperServerConfiguration, Clock, ExternalServiceCredentialsGenerator> factory) {
    this.externalService = requireNonNull(externalService);
    this.generatorFactory = requireNonNull(factory);
  }

  public static Map<ExternalServiceType, ExternalServiceCredentialsGenerator> createExternalServiceList(
      final WhisperServerConfiguration chatConfiguration,
      final Clock clock) {
    return Arrays.stream(values())
        .map(esd -> Pair.of(esd.externalService, esd.generatorFactory().apply(chatConfiguration, clock)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  public BiFunction<WhisperServerConfiguration, Clock, ExternalServiceCredentialsGenerator> generatorFactory() {
    return generatorFactory;
  }

  ExternalServiceType externalService() {
    return externalService;
  }
}
