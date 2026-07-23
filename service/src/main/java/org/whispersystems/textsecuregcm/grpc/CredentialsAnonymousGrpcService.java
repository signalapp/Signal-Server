/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.signal.chat.credentials.AuthCheckResult;
import org.signal.chat.credentials.CheckSvrCredentialsRequest;
import org.signal.chat.credentials.CheckSvrCredentialsResponse;
import org.signal.chat.credentials.SimpleCredentialsAnonymousGrpc;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsSelector;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class CredentialsAnonymousGrpcService extends SimpleCredentialsAnonymousGrpc.CredentialsAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final ExternalServiceCredentialsGenerator svrCredentialsGenerator;

  private static final long MAX_SVR_PASSWORD_AGE_SECONDS = TimeUnit.DAYS.toSeconds(30);

  public CredentialsAnonymousGrpcService(final AccountsManager accountsManager,
      final ExternalServiceCredentialsGenerator svrCredentialsGenerator) {

    this.svrCredentialsGenerator = svrCredentialsGenerator;
    this.accountsManager = accountsManager;
  }

  @Override
  public CheckSvrCredentialsResponse checkSvrCredentials(final CheckSvrCredentialsRequest request) {
    final List<String> tokens = request.getPasswordsList().stream().distinct().toList();
    final List<ExternalServiceCredentialsSelector.CredentialInfo> credentials = ExternalServiceCredentialsSelector.check(
        new HashSet<>(tokens),
        svrCredentialsGenerator,
        MAX_SVR_PASSWORD_AGE_SECONDS);

    // the username associated with the provided number
    final Optional<String> maybeUsername = accountsManager.getByE164(request.getNumber())
        .map(Account::getUuid)
        .map(svrCredentialsGenerator::generateForUuid)
        .map(ExternalServiceCredentials::username);

    final CheckSvrCredentialsResponse.Builder builder = CheckSvrCredentialsResponse.newBuilder();

    for (ExternalServiceCredentialsSelector.CredentialInfo credentialInfo : credentials) {
      final AuthCheckResult authCheckResult;

      if (!credentialInfo.valid()) {
        authCheckResult = AuthCheckResult.AUTH_CHECK_RESULT_INVALID;
      } else {
        final String username = credentialInfo.credentials().username();
        // does this credential match the account id for the e164 provided in the request?
        authCheckResult = maybeUsername.map(username::equals).orElse(false)
            ? AuthCheckResult.AUTH_CHECK_RESULT_MATCH
            : AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH;
      }

      builder.putMatches(credentialInfo.token(), authCheckResult);
    }

    return builder.build();
  }
}
