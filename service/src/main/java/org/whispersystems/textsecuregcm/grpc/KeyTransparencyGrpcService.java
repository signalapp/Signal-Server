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
import org.signal.keytransparency.client.SearchRequest;
import org.signal.keytransparency.client.SearchResponse;
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
  public SearchResponse search(final SearchRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencySearchLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    return client.search(validateSearchRequest(request));
  }

  @Override
  public MonitorResponse monitor(final MonitorRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencyMonitorLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    return client.monitor(validateMonitorRequest(request));
  }

  @Override
  public DistinguishedResponse distinguished(final DistinguishedRequest request) throws RateLimitExceededException {
    rateLimiters.getKeyTransparencyDistinguishedLimiter().validate(RequestAttributesUtil.getRemoteAddress().getHostAddress());
    // A client's very first distinguished request will not have a "last" parameter
    if (request.hasLast() && request.getLast() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Last tree head size must be positive").asRuntimeException();
    }
    return client.distinguished(request);
  }

  private SearchRequest validateSearchRequest(final SearchRequest request) {
    if (request.hasE164SearchRequest()) {
      final E164SearchRequest e164SearchRequest = request.getE164SearchRequest();
      if (e164SearchRequest.getUnidentifiedAccessKey().isEmpty() != e164SearchRequest.getE164().isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("Unidentified access key and E164 must be provided together or not at all").asRuntimeException();
      }
    }

    if (!request.getConsistency().hasDistinguished()) {
      throw Status.INVALID_ARGUMENT.withDescription("Must provide distinguished tree head size").asRuntimeException();
    }

    validateConsistencyParameters(request.getConsistency());
    return request;
  }

  private MonitorRequest validateMonitorRequest(final MonitorRequest request) {
    final AciMonitorRequest aciMonitorRequest = request.getAci();

    try {
      AciServiceIdentifier.fromBytes(aciMonitorRequest.getAci().toByteArray());
    } catch (IllegalArgumentException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid ACI").asRuntimeException();
    }
    if (aciMonitorRequest.getEntryPosition() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Aci entry position must be positive").asRuntimeException();
    }
    if (aciMonitorRequest.getCommitmentIndex().size() != COMMITMENT_INDEX_LENGTH) {
      throw Status.INVALID_ARGUMENT.withDescription("Aci commitment index must be 32 bytes").asRuntimeException();
    }

    if (request.hasUsernameHash()) {
      final UsernameHashMonitorRequest usernameHashMonitorRequest = request.getUsernameHash();
      if (usernameHashMonitorRequest.getUsernameHash().isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("Username hash cannot be empty").asRuntimeException();
      }
      if (usernameHashMonitorRequest.getUsernameHash().size() != AccountController.USERNAME_HASH_LENGTH) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid username hash length").asRuntimeException();
      }
      if (usernameHashMonitorRequest.getEntryPosition() <= 0) {
        throw Status.INVALID_ARGUMENT.withDescription("Username hash entry position must be positive").asRuntimeException();
      }
      if (usernameHashMonitorRequest.getCommitmentIndex().size() != COMMITMENT_INDEX_LENGTH) {
        throw Status.INVALID_ARGUMENT.withDescription("Username hash commitment index must be 32 bytes").asRuntimeException();
      }
    }

    if (request.hasE164()) {
      final E164MonitorRequest e164MonitorRequest = request.getE164();
      if (e164MonitorRequest.getE164().isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("E164 cannot be empty").asRuntimeException();
      }
      if (e164MonitorRequest.getEntryPosition() <= 0) {
        throw Status.INVALID_ARGUMENT.withDescription("E164 entry position must be positive").asRuntimeException();
      }
      if (e164MonitorRequest.getCommitmentIndex().size() != COMMITMENT_INDEX_LENGTH) {
        throw Status.INVALID_ARGUMENT.withDescription("E164 commitment index must be 32 bytes").asRuntimeException();
      }
    }

    if (!request.getConsistency().hasDistinguished() || !request.getConsistency().hasLast()) {
      throw Status.INVALID_ARGUMENT.withDescription("Must provide distinguished and last tree head sizes").asRuntimeException();
    }

    validateConsistencyParameters(request.getConsistency());
    return request;
  }

  private static void validateConsistencyParameters(final ConsistencyParameters consistency) {
    if (consistency.getDistinguished() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Distinguished tree head size must be positive").asRuntimeException();
    }

    if (consistency.hasLast() && consistency.getLast() <= 0) {
      throw Status.INVALID_ARGUMENT.withDescription("Last tree head size must be positive").asRuntimeException();
    }
  }
}
