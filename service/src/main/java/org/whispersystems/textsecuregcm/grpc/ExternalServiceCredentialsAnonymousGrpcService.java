/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.signal.chat.credentials.AuthCheckResult;
import org.signal.chat.credentials.CheckSvrCredentialsRequest;
import org.signal.chat.credentials.CheckSvrCredentialsResponse;
import org.signal.chat.credentials.ReactorExternalServiceCredentialsAnonymousGrpc;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsSelector;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class ExternalServiceCredentialsAnonymousGrpcService extends
    ReactorExternalServiceCredentialsAnonymousGrpc.ExternalServiceCredentialsAnonymousImplBase {

  private static final long MAX_SVR_PASSWORD_AGE_SECONDS = TimeUnit.DAYS.toSeconds(30);

  private final ExternalServiceCredentialsGenerator svrCredentialsGenerator;

  private final AccountsManager accountsManager;


  public static ExternalServiceCredentialsAnonymousGrpcService create(
      final AccountsManager accountsManager,
      final WhisperServerConfiguration chatConfiguration) {
    return new ExternalServiceCredentialsAnonymousGrpcService(
        accountsManager,
        ExternalServiceDefinitions.SVR.generatorFactory().apply(chatConfiguration, Clock.systemUTC())
    );
  }

  @VisibleForTesting
  ExternalServiceCredentialsAnonymousGrpcService(
      final AccountsManager accountsManager,
      final ExternalServiceCredentialsGenerator svrCredentialsGenerator) {
    this.accountsManager = requireNonNull(accountsManager);
    this.svrCredentialsGenerator = requireNonNull(svrCredentialsGenerator);
  }

  @Override
  public Mono<CheckSvrCredentialsResponse> checkSvrCredentials(final CheckSvrCredentialsRequest request) {
    final List<String> tokens = request.getPasswordsList();
    final List<ExternalServiceCredentialsSelector.CredentialInfo> credentials = ExternalServiceCredentialsSelector.check(
        tokens,
        svrCredentialsGenerator,
        MAX_SVR_PASSWORD_AGE_SECONDS);

    return Mono.fromFuture(() -> accountsManager.getByE164Async(request.getNumber()))
        // the username associated with the provided number
        .map(maybeAccount -> maybeAccount.map(Account::getUuid)
            .map(svrCredentialsGenerator::generateForUuid)
            .map(ExternalServiceCredentials::username))
        .flatMapMany(maybeMatchingUsername -> Flux.fromIterable(credentials)
            .map(credential -> Tuples.of(maybeMatchingUsername, credential)))
        .reduce(CheckSvrCredentialsResponse.newBuilder(), ((builder, usernameAndCredentialInfo) -> {
          final Optional<String> matchingUsername = usernameAndCredentialInfo.getT1();
          final ExternalServiceCredentialsSelector.CredentialInfo credentialInfo = usernameAndCredentialInfo.getT2();

          final AuthCheckResult authCheckResult;
          if (!credentialInfo.valid()) {
            authCheckResult = AuthCheckResult.AUTH_CHECK_RESULT_INVALID;
          } else {
            final String username = credentialInfo.credentials().username();
            // does this credential match the account id for the e164 provided in the request?
            authCheckResult = matchingUsername.map(username::equals).orElse(false)
                ? AuthCheckResult.AUTH_CHECK_RESULT_MATCH
                : AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH;
          }
          return builder.putMatches(credentialInfo.token(), authCheckResult);
        }))
        .map(CheckSvrCredentialsResponse.Builder::build);
  }
}
