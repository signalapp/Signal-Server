/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.signal.keytransparency.client.AciMonitorRequest;
import org.signal.keytransparency.client.ConsistencyParameters;
import org.signal.keytransparency.client.DistinguishedRequest;
import org.signal.keytransparency.client.DistinguishedResponse;
import org.signal.keytransparency.client.E164MonitorRequest;
import org.signal.keytransparency.client.E164SearchRequest;
import org.signal.keytransparency.client.KeyTransparencyQueryServiceGrpc;
import org.signal.keytransparency.client.MonitorRequest;
import org.signal.keytransparency.client.MonitorResponse;
import org.signal.keytransparency.client.SearchRequest;
import org.signal.keytransparency.client.SearchResponse;
import org.signal.keytransparency.client.UsernameHashMonitorRequest;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.keytransparency.KeyTransparencyServiceClient;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyControllerTest.ACI;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyControllerTest.ACI_IDENTITY_KEY;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyControllerTest.NUMBER;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyControllerTest.UNIDENTIFIED_ACCESS_KEY;
import static org.whispersystems.textsecuregcm.controllers.KeyTransparencyControllerTest.USERNAME_HASH;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;
import static org.whispersystems.textsecuregcm.grpc.KeyTransparencyGrpcService.COMMITMENT_INDEX_LENGTH;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class KeyTransparencyGrpcServiceTest extends SimpleBaseGrpcTest<KeyTransparencyGrpcService, KeyTransparencyQueryServiceGrpc.KeyTransparencyQueryServiceBlockingStub>{
  @Mock
  private KeyTransparencyServiceClient keyTransparencyServiceClient;
  @Mock
  private RateLimiter rateLimiter;

  @Override
  protected KeyTransparencyGrpcService createServiceBeforeEachTest() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getKeyTransparencySearchLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getKeyTransparencyDistinguishedLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getKeyTransparencyMonitorLimiter()).thenReturn(rateLimiter);

    return new KeyTransparencyGrpcService(rateLimiters, keyTransparencyServiceClient);
  }

  @Override
  protected KeyTransparencyQueryServiceGrpc.KeyTransparencyQueryServiceBlockingStub createStub(final Channel channel) {
    return KeyTransparencyQueryServiceGrpc.newBlockingStub(channel);
  }

  @Test
  void searchSuccess() throws RateLimitExceededException {
    when(keyTransparencyServiceClient.search(any())).thenReturn(SearchResponse.getDefaultInstance());
    Mockito.doNothing().when(rateLimiter).validate(any(String.class));
    final SearchRequest request = SearchRequest.newBuilder()
        .setAci(ByteString.copyFrom(ACI.toCompactByteArray()))
        .setAciIdentityKey(ByteString.copyFrom(ACI_IDENTITY_KEY.serialize()))
        .setConsistency(ConsistencyParameters.newBuilder()
            .setDistinguished(10)
            .build())
        .build();

    assertDoesNotThrow(() -> unauthenticatedServiceStub().search(request));
    verify(keyTransparencyServiceClient, times(1)).search(eq(request));
  }

  @ParameterizedTest
  @MethodSource
  void searchInvalidRequest(final Optional<byte[]> aciServiceIdentifier,
      final Optional<IdentityKey> aciIdentityKey,
      final Optional<String> e164,
      final Optional<byte[]> unidentifiedAccessKey,
      final Optional<byte[]> usernameHash,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {

    final SearchRequest.Builder requestBuilder = SearchRequest.newBuilder();

    aciServiceIdentifier.ifPresent(v -> requestBuilder.setAci(ByteString.copyFrom(v)));
    aciIdentityKey.ifPresent(v -> requestBuilder.setAciIdentityKey(ByteString.copyFrom(v.serialize())));
    usernameHash.ifPresent(v -> requestBuilder.setUsernameHash(ByteString.copyFrom(v)));

    final E164SearchRequest.Builder e164RequestBuilder = E164SearchRequest.newBuilder();

    e164.ifPresent(e164RequestBuilder::setE164);
    unidentifiedAccessKey.ifPresent(v -> e164RequestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(v)));
    requestBuilder.setE164SearchRequest(e164RequestBuilder.build());

    final ConsistencyParameters.Builder consistencyBuilder = ConsistencyParameters.newBuilder();
    distinguishedTreeHeadSize.ifPresent(consistencyBuilder::setDistinguished);
    lastTreeHeadSize.ifPresent(consistencyBuilder::setLast);
    requestBuilder.setConsistency(consistencyBuilder.build());

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().search(requestBuilder.build()));
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  private static Stream<Arguments> searchInvalidRequest() {
    byte[] aciBytes = ACI.toCompactByteArray();
    return Stream.of(
        Arguments.argumentSet("Empty ACI", Optional.empty(), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L)),
        Arguments.argumentSet("Null ACI identity key", Optional.of(aciBytes), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L)),
        Arguments.argumentSet("Invalid ACI", Optional.of(new byte[15]), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L)),
        Arguments.argumentSet("Non-positive consistency.last", Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(0L), Optional.of(4L)),
        Arguments.argumentSet("consistency.distinguished not provided",Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
        Arguments.argumentSet("Non-positive consistency.distinguished",Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(0L)),
        Arguments.argumentSet("E164 can't be provided without an unidentified access key", Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.of(NUMBER), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L)),
        Arguments.argumentSet("Unidentified access key can't be provided without E164", Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.of(UNIDENTIFIED_ACCESS_KEY), Optional.empty(), Optional.empty(), Optional.of(4L)),
        Arguments.argumentSet("Invalid username hash", Optional.of(aciBytes), Optional.of(ACI_IDENTITY_KEY), Optional.empty(), Optional.empty(), Optional.of(new byte[19]), Optional.empty(), Optional.of(4L))
    );
  }

  @Test
  void searchRatelimited() throws RateLimitExceededException {
    final Duration retryAfterDuration = Duration.ofMinutes(7);
    Mockito.doThrow(new RateLimitExceededException(retryAfterDuration)).when(rateLimiter).validate(any(String.class));

    final SearchRequest request = SearchRequest.newBuilder()
        .setAci(ByteString.copyFrom(ACI.toCompactByteArray()))
        .setAciIdentityKey(ByteString.copyFrom(ACI_IDENTITY_KEY.serialize()))
        .setConsistency(ConsistencyParameters.newBuilder()
            .setDistinguished(10)
            .build())
        .build();
    assertRateLimitExceeded(retryAfterDuration, () -> unauthenticatedServiceStub().search(request));
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @Test
  void monitorSuccess() {
    when(keyTransparencyServiceClient.monitor(any())).thenReturn(MonitorResponse.getDefaultInstance());
    when(rateLimiter.validateReactive(any(String.class)))
        .thenReturn(Mono.empty());
    final AciMonitorRequest aciMonitorRequest = AciMonitorRequest.newBuilder()
        .setAci(ByteString.copyFrom(ACI.toCompactByteArray()))
        .setCommitmentIndex(ByteString.copyFrom(new byte[COMMITMENT_INDEX_LENGTH]))
        .setEntryPosition(10)
        .build();

    final MonitorRequest request = MonitorRequest.newBuilder()
        .setAci(aciMonitorRequest)
        .setConsistency(ConsistencyParameters.newBuilder()
            .setDistinguished(10)
            .setLast(10)
            .build())
        .build();

    assertDoesNotThrow(() -> unauthenticatedServiceStub().monitor(request));
    verify(keyTransparencyServiceClient, times(1)).monitor(eq(request));
  }

  @ParameterizedTest
  @MethodSource
  void monitorInvalidRequest(final Optional<AciMonitorRequest> aciMonitorRequest,
      final Optional<E164MonitorRequest> e164MonitorRequest,
      final Optional<UsernameHashMonitorRequest> usernameHashMonitorRequest,
      final Optional<Long> lastTreeHeadSize,
      final Optional<Long> distinguishedTreeHeadSize) {

    final MonitorRequest.Builder requestBuilder = MonitorRequest.newBuilder();

    aciMonitorRequest.ifPresent(requestBuilder::setAci);
    e164MonitorRequest.ifPresent(requestBuilder::setE164);
    usernameHashMonitorRequest.ifPresent(requestBuilder::setUsernameHash);

    final ConsistencyParameters.Builder consistencyBuilder = ConsistencyParameters.newBuilder();
    lastTreeHeadSize.ifPresent(consistencyBuilder::setLast);
    distinguishedTreeHeadSize.ifPresent(consistencyBuilder::setDistinguished);

    requestBuilder.setConsistency(consistencyBuilder.build());

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().monitor(requestBuilder.build()));
  }

  private static Stream<Arguments> monitorInvalidRequest() {
    final Optional<AciMonitorRequest> validAciMonitorRequest = Optional.of(constructAciMonitorRequest(ACI.toCompactByteArray(), new byte[32], 10));
    return Stream.of(
        Arguments.argumentSet("ACI monitor request can't be unset", Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("ACI can't be empty",Optional.of(AciMonitorRequest.newBuilder().build()), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Empty ACI on ACI monitor request",Optional.of(constructAciMonitorRequest(new byte[0], new byte[32], 10)), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid ACI", Optional.of(constructAciMonitorRequest(new byte[15], new byte[32], 10)), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid commitment index on ACI monitor request", Optional.of(constructAciMonitorRequest(ACI.toCompactByteArray(), new byte[31], 10)), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid entry position on ACI monitor request", Optional.of(constructAciMonitorRequest(ACI.toCompactByteArray(), new byte[32], 0)), Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("E164 can't be blank", validAciMonitorRequest, Optional.of(constructE164MonitorRequest("", new byte[32], 10)), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid commitment index on E164 monitor request", validAciMonitorRequest, Optional.of(constructE164MonitorRequest(NUMBER, new byte[31], 10)), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid entry position on E164 monitor request", validAciMonitorRequest, Optional.of(constructE164MonitorRequest(NUMBER, new byte[32], 0)), Optional.empty(), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Username hash can't be empty", validAciMonitorRequest, Optional.empty(), Optional.of(constructUsernameHashMonitorRequest(new byte[0], new byte[32], 10)), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid username hash length", validAciMonitorRequest, Optional.empty(), Optional.of(constructUsernameHashMonitorRequest(new byte[31], new byte[32], 10)), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid commitment index on username hash monitor request", validAciMonitorRequest, Optional.empty(), Optional.of(constructUsernameHashMonitorRequest(USERNAME_HASH, new byte[31], 10)), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("Invalid entry position on username hash monitor request", validAciMonitorRequest, Optional.empty(), Optional.of(constructUsernameHashMonitorRequest(USERNAME_HASH, new byte[32], 0)), Optional.of(4L), Optional.of(4L)),
        Arguments.argumentSet("consistency.last must be provided", validAciMonitorRequest, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(4L),
        Arguments.argumentSet("consistency.last must be positive", validAciMonitorRequest, Optional.empty(), Optional.empty(), Optional.of(0L), Optional.of(4L)),
        Arguments.argumentSet("consistency.distinguished must be provided", validAciMonitorRequest, Optional.empty(), Optional.empty(), Optional.of(4L)), Optional.empty()),
        Arguments.argumentSet("consistency.distinguished must be positive", validAciMonitorRequest, Optional.empty(), Optional.empty(), Optional.of(4L), Optional.of(0L))
    );
  }

  @Test
  void monitorRatelimited() throws RateLimitExceededException {
    final Duration retryAfterDuration = Duration.ofMinutes(7);
    Mockito.doThrow(new RateLimitExceededException(retryAfterDuration)).when(rateLimiter).validate(any(String.class));

    final AciMonitorRequest aciMonitorRequest = AciMonitorRequest.newBuilder()
        .setAci(ByteString.copyFrom(ACI.toCompactByteArray()))
        .setCommitmentIndex(ByteString.copyFrom(new byte[COMMITMENT_INDEX_LENGTH]))
        .setEntryPosition(10)
        .build();

    final MonitorRequest request = MonitorRequest.newBuilder()
        .setAci(aciMonitorRequest)
        .setConsistency(ConsistencyParameters.newBuilder()
            .setDistinguished(10)
            .setLast(10)
            .build())
        .build();
    assertRateLimitExceeded(retryAfterDuration, () -> unauthenticatedServiceStub().monitor(request));
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @Test
  void distinguishedSuccess() {
    when(keyTransparencyServiceClient.distinguished(any())).thenReturn(DistinguishedResponse.getDefaultInstance());
    when(rateLimiter.validateReactive(any(String.class)))
        .thenReturn(Mono.empty());
    final DistinguishedRequest request = DistinguishedRequest.newBuilder().build();

    assertDoesNotThrow(() -> unauthenticatedServiceStub().distinguished(request));
    verify(keyTransparencyServiceClient, times(1)).distinguished(eq(request));
  }

  @Test
  void distinguishedInvalidRequest() {
    final DistinguishedRequest request = DistinguishedRequest.newBuilder()
        .setLast(0)
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().distinguished(request));
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  @Test
  void distinguishedRatelimited() throws RateLimitExceededException {
    final Duration retryAfterDuration = Duration.ofMinutes(7);
    Mockito.doThrow(new RateLimitExceededException(retryAfterDuration)).when(rateLimiter).validate(any(String.class));

    final DistinguishedRequest request = DistinguishedRequest.newBuilder()
        .setLast(10)
        .build();

    assertRateLimitExceeded(retryAfterDuration, () -> unauthenticatedServiceStub().distinguished(request));
    verifyNoInteractions(keyTransparencyServiceClient);
  }

  private static AciMonitorRequest constructAciMonitorRequest(final byte[] aci, final byte[] commitmentIndex, final long entryPosition) {
    return AciMonitorRequest.newBuilder()
        .setAci(ByteString.copyFrom(aci))
        .setCommitmentIndex(ByteString.copyFrom(commitmentIndex))
        .setEntryPosition(entryPosition)
        .build();
  }

  private static E164MonitorRequest constructE164MonitorRequest(final String e164, final byte[] commitmentIndex, final long entryPosition) {
    return E164MonitorRequest.newBuilder()
        .setE164(e164)
        .setCommitmentIndex(ByteString.copyFrom(commitmentIndex))
        .setEntryPosition(entryPosition)
        .build();
  }

  private static UsernameHashMonitorRequest constructUsernameHashMonitorRequest(final byte[] usernameHash, final byte[] commitmentIndex, final long entryPosition) {
    return UsernameHashMonitorRequest.newBuilder()
        .setUsernameHash(ByteString.copyFrom(usernameHash))
        .setCommitmentIndex(ByteString.copyFrom(commitmentIndex))
        .setEntryPosition(entryPosition)
        .build();
  }
}
