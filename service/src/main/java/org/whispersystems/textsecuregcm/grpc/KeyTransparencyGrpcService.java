/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import org.signal.keytransparency.client.AciMonitorRequest;
import org.signal.keytransparency.client.ConsistencyParameters;
import org.signal.keytransparency.client.DistinguishedRequest;
import org.signal.keytransparency.client.DistinguishedResponse;
import org.signal.keytransparency.client.E164MonitorRequest;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.MonitorRequest;
import org.signal.keytransparency.client.MonitorResponse;
import org.signal.keytransparency.client.MonitorResponseV2;
import org.signal.keytransparency.client.SearchRequest;
import org.signal.keytransparency.client.SearchResponse;
import org.signal.keytransparency.client.SearchResponseV2;
import org.signal.keytransparency.client.SimpleKeyTransparencyQueryServiceGrpc;
import org.signal.keytransparency.client.UsernameHashMonitorRequest;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.keytransparency.KeyTransparencyServiceClient;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

public class KeyTransparencyGrpcService extends
    SimpleKeyTransparencyQueryServiceGrpc.KeyTransparencyQueryServiceImplBase {
  @VisibleForTesting
  static final int COMMITMENT_INDEX_LENGTH = 32;
  private final RateLimiters rateLimiters;
  private final KeyTransparencyServiceClient client;

  public KeyTransparencyGrpcService(final RateLimiters rateLimiters,
      final KeyTransparencyServiceClient client) {
    this.rateLimiters = rateLimiters;
    this.client = client;
  }

  @Override
  public SearchResponseV2 searchV2(final SearchRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencySearchLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    return client.search(validateSearchRequest(request));
  }

  @Override
  public MonitorResponseV2 monitorV2(final MonitorRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencyMonitorLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    return client.monitor(validateMonitorRequest(request));
  }

  @Override
  public DistinguishedResponse distinguishedV2(final DistinguishedRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencyDistinguishedLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    return client.distinguished(request);
  }

  private SearchRequest validateSearchRequest(final SearchRequest request) {
    validateAci(request.getAci().toByteArray());

    if (request.hasE164SearchRequest()) {
      final E164SearchRequest e164SearchRequest = request.getE164SearchRequest();
      if (e164SearchRequest.getUnidentifiedAccessKey().isEmpty() != e164SearchRequest.getE164().isEmpty()) {
        throw GrpcExceptions.fieldViolation("e164_search_request", "Unidentified access key and E164 must be provided together or not at all");
      }
    }

    return request;
  }

  private void validateAci(final byte[] aci) {
    try {
      AciServiceIdentifier.fromBytes(aci);
    } catch (IllegalArgumentException e) {
      throw GrpcExceptions.fieldViolation("aci", "Invalid ACI");
    }
  }

  private MonitorRequest validateMonitorRequest(final MonitorRequest request) {
    validateAci(request.getAci().getAci().toByteArray());

    if (!request.getConsistency().hasLast()) {
      throw GrpcExceptions.fieldViolation("consistency_last", "Must provide distinguished and last tree head sizes");
    }

    return request;
  }
}
