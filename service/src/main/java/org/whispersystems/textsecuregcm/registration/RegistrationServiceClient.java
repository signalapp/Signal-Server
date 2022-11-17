package org.whispersystems.textsecuregcm.registration;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.google.protobuf.ByteString;
import io.dropwizard.lifecycle.Managed;
import io.grpc.ChannelCredentials;
import io.grpc.Deadline;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.signal.registration.rpc.CheckVerificationCodeRequest;
import org.signal.registration.rpc.CheckVerificationCodeResponse;
import org.signal.registration.rpc.CreateRegistrationSessionRequest;
import org.signal.registration.rpc.RegistrationServiceGrpc;
import org.signal.registration.rpc.SendVerificationCodeRequest;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;

public class RegistrationServiceClient implements Managed {

  private final ManagedChannel channel;
  private final RegistrationServiceGrpc.RegistrationServiceFutureStub stub;
  private final Executor callbackExecutor;

  public RegistrationServiceClient(final String host,
      final int port,
      final String apiKey,
      final String caCertificatePem,
      final Executor callbackExecutor) throws IOException {

    try (final ByteArrayInputStream certificateInputStream = new ByteArrayInputStream(caCertificatePem.getBytes(StandardCharsets.UTF_8))) {
      final ChannelCredentials tlsChannelCredentials = TlsChannelCredentials.newBuilder()
          .trustManager(certificateInputStream)
          .build();

      this.channel = Grpc.newChannelBuilderForAddress(host, port, tlsChannelCredentials).build();
    }

    this.stub = RegistrationServiceGrpc.newFutureStub(channel)
        .withCallCredentials(new ApiKeyCallCredentials(apiKey));

    this.callbackExecutor = callbackExecutor;
  }

  public CompletableFuture<byte[]> createRegistrationSession(final Phonenumber.PhoneNumber phoneNumber, final Duration timeout) {
    final long e164 = Long.parseLong(
        PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164).substring(1));

    return toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .createSession(CreateRegistrationSessionRequest.newBuilder()
            .setE164(e164)
            .build()))
        .thenApply(response -> switch (response.getResponseCase()) {
          case SESSION_METADATA -> response.getSessionMetadata().getSessionId().toByteArray();

          case ERROR -> {
            switch (response.getError().getErrorType()) {
              case ERROR_TYPE_RATE_LIMITED -> throw new CompletionException(new RateLimitExceededException(Duration.ofSeconds(response.getError().getRetryAfterSeconds())));
              default -> throw new RuntimeException("Unrecognized error type from registration service: " + response.getError().getErrorType());
            }
          }

          case RESPONSE_NOT_SET -> throw new RuntimeException("No response from registration service");
        });
  }

  public CompletableFuture<byte[]> sendRegistrationCode(final byte[] sessionId,
      final MessageTransport messageTransport,
      final ClientType clientType,
      @Nullable final String acceptLanguage,
      final Duration timeout) {

    final SendVerificationCodeRequest.Builder requestBuilder = SendVerificationCodeRequest.newBuilder()
        .setSessionId(ByteString.copyFrom(sessionId))
        .setTransport(getRpcMessageTransport(messageTransport))
        .setClientType(getRpcClientType(clientType));

    if (StringUtils.isNotBlank(acceptLanguage)) {
      requestBuilder.setAcceptLanguage(acceptLanguage);
    }

    return toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .sendVerificationCode(requestBuilder.build()))
        .thenApply(response -> response.getSessionId().toByteArray());
  }

  public CompletableFuture<Boolean> checkVerificationCode(final byte[] sessionId,
      final String verificationCode,
      final Duration timeout) {

    return toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .checkVerificationCode(CheckVerificationCodeRequest.newBuilder()
        .setSessionId(ByteString.copyFrom(sessionId))
        .setVerificationCode(verificationCode)
        .build()))
        .thenApply(CheckVerificationCodeResponse::getVerified);
  }

  private static Deadline toDeadline(final Duration timeout) {
    return Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static org.signal.registration.rpc.ClientType getRpcClientType(final ClientType clientType) {
    return switch (clientType) {
      case IOS -> org.signal.registration.rpc.ClientType.CLIENT_TYPE_IOS;
      case ANDROID_WITH_FCM -> org.signal.registration.rpc.ClientType.CLIENT_TYPE_ANDROID_WITH_FCM;
      case ANDROID_WITHOUT_FCM -> org.signal.registration.rpc.ClientType.CLIENT_TYPE_ANDROID_WITHOUT_FCM;
      case UNKNOWN -> org.signal.registration.rpc.ClientType.CLIENT_TYPE_UNSPECIFIED;
    };
  }

  private static org.signal.registration.rpc.MessageTransport getRpcMessageTransport(final MessageTransport transport) {
    return switch (transport) {
      case SMS -> org.signal.registration.rpc.MessageTransport.MESSAGE_TRANSPORT_SMS;
      case VOICE -> org.signal.registration.rpc.MessageTransport.MESSAGE_TRANSPORT_VOICE;
    };
  }

  private <T> CompletableFuture<T> toCompletableFuture(final ListenableFuture<T> listenableFuture) {
    final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    Futures.addCallback(listenableFuture, new FutureCallback<T>() {
      @Override
      public void onSuccess(@Nullable final T result) {
        completableFuture.complete(result);
      }

      @Override
      public void onFailure(final Throwable throwable) {
        completableFuture.completeExceptionally(throwable);
      }
    }, callbackExecutor);

    return completableFuture;
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }
}
