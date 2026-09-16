/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.signal.keytransparency.client.DistinguishedRequest;
import org.signal.keytransparency.client.DistinguishedResponse;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.MonitorRequest;
import org.signal.keytransparency.client.MonitorResponseV2;
import org.signal.keytransparency.client.SearchRequest;
import org.signal.keytransparency.client.SearchResponseV2;
import org.signal.keytransparency.client.SimpleKeyTransparencyQueryServiceGrpc;
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

  @Override
  public Throwable mapException(final Throwable throwable) {
    // Reconstruct the exception so that we can override the backing service domain with chat's own domain.
    if (throwable instanceof StatusRuntimeException s) {
      final ErrorInfo.Builder errInfoBuilder = ErrorInfo.newBuilder()
          .setDomain(GrpcExceptions.DOMAIN);

      final Status statusProto = StatusProto.fromStatusAndTrailers(s.getStatus(), s.getTrailers());

      ErrorUtil.errorInfo(statusProto)
          .map(ErrorInfo::getReason)
          .ifPresent(errInfoBuilder::setReason);

      return StatusProto.toStatusRuntimeException(Status.newBuilder()
          // See https://github.com/signalapp/key-transparency-server/blob/30b7a2a40604fbe04545d47f26e4c5a35f5d3161/cmd/kt-server/errors.go#L32
          // for a list of error codes that the key transparency service can generate.
          .setCode(statusProto.getCode())
          .setMessage(statusProto.getMessage())
          .addDetails(Any.pack(errInfoBuilder.build()))
          .build());
    }
    return throwable;
  }
}
