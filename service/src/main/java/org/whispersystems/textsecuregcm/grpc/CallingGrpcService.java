/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.signal.chat.calling.GetCallingRelaysRequest;
import org.signal.chat.calling.GetCallingRelaysResponse;
import org.signal.chat.calling.SimpleCallingGrpc;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

public class CallingGrpcService extends SimpleCallingGrpc.CallingImplBase {

  private final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager;
  private final RateLimiters rateLimiters;

  public CallingGrpcService(final CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager,
      final RateLimiters rateLimiters) {

    this.cloudflareTurnCredentialsManager = cloudflareTurnCredentialsManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public GetCallingRelaysResponse getCallingRelays(final GetCallingRelaysRequest request)
      throws RateLimitExceededException, IOException {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    rateLimiters.getCallEndpointLimiter().validate(authenticatedDevice.accountIdentifier());

    final TurnToken turnToken =
        cloudflareTurnCredentialsManager.retrieveFromCloudflare(authenticatedDevice.accountIdentifier());

    final GetCallingRelaysResponse.Relay.Builder relayBuilder = GetCallingRelaysResponse.Relay.newBuilder()
        .setUsername(turnToken.username())
        .setPassword(turnToken.password())
        .setCredentialTtlSeconds(turnToken.ttlSeconds());

    if (!turnToken.urls().isEmpty()) {
      relayBuilder.setHostnameUrls(turnToken.urls().stream()
          .collect(GetCallingRelaysResponse.HostnameUrlList::newBuilder,
              GetCallingRelaysResponse.HostnameUrlList.Builder::addUrls,
              (a, b) -> a.mergeFrom(b.build())));
    }

    if (!turnToken.urlsWithIps().isEmpty()) {
      relayBuilder.setIpUrls(turnToken.urlsWithIps().stream()
          .collect(() -> {
                final GetCallingRelaysResponse.IpUrlList.Builder builder =
                    GetCallingRelaysResponse.IpUrlList.newBuilder();

                if (StringUtils.isNotBlank(turnToken.hostname())) {
                  builder.setHostname(turnToken.hostname());
                }

                return builder;
              },
              GetCallingRelaysResponse.IpUrlList.Builder::addUrls,
              (a, b) -> a.mergeFrom(b.build())));
    }

    return GetCallingRelaysResponse.newBuilder()
        .addRelays(relayBuilder.build())
        .build();
  }
}
