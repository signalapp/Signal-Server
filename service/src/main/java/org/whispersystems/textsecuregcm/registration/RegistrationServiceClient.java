package org.whispersystems.textsecuregcm.registration;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.i18n.phonenumbers.NumberParseException;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.signal.registration.rpc.CheckVerificationCodeRequest;
import org.signal.registration.rpc.CreateRegistrationSessionRequest;
import org.signal.registration.rpc.GetRegistrationSessionMetadataRequest;
import org.signal.registration.rpc.RegistrationServiceGrpc;
import org.signal.registration.rpc.SendVerificationCodeRequest;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.RegistrationSession;

public class RegistrationServiceClient implements Managed {

  private final ManagedChannel channel;
  private final RegistrationServiceGrpc.RegistrationServiceFutureStub stub;
  private final Executor callbackExecutor;

  /**
   * @param from an e164 in a {@code long} representation e.g. {@code 18005550123}
   * @return the e164 in a {@code String} representation (e.g. {@code "+18005550123"})
   * @throws IllegalArgumentException if the number cannot be parsed to a string
   */
  static String convertNumeralE164ToString(long from) {

    try {
      final Phonenumber.PhoneNumber phoneNumber = PhoneNumberUtil.getInstance()
          .parse("+" + from, null);
      return PhoneNumberUtil.getInstance()
          .format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164);
    } catch (final NumberParseException e) {
      throw new IllegalArgumentException("could not parse to phone number", e);
    }

  }

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
              case CREATE_REGISTRATION_SESSION_ERROR_TYPE_RATE_LIMITED -> throw new CompletionException(new RateLimitExceededException(Duration.ofSeconds(response.getError().getRetryAfterSeconds())));
              case CREATE_REGISTRATION_SESSION_ERROR_TYPE_ILLEGAL_PHONE_NUMBER -> throw new IllegalArgumentException();
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
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case SEND_VERIFICATION_CODE_ERROR_TYPE_RATE_LIMITED ->
                  throw new CompletionException(new RateLimitExceededException(Duration.ofSeconds(response.getError().getRetryAfterSeconds())));

              default -> throw new CompletionException(new RuntimeException("Failed to send verification code: " + response.getError().getErrorType()));
            }
          } else {
            return response.getSessionId().toByteArray();
          }
      });
  }

  public CompletableFuture<Boolean> checkVerificationCode(final byte[] sessionId,
      final String verificationCode,
      final Duration timeout) {

    return toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .checkVerificationCode(CheckVerificationCodeRequest.newBuilder()
            .setSessionId(ByteString.copyFrom(sessionId))
            .setVerificationCode(verificationCode)
            .build()))
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case CHECK_VERIFICATION_CODE_ERROR_TYPE_RATE_LIMITED ->
                  throw new CompletionException(new RateLimitExceededException(Duration.ofSeconds(response.getError().getRetryAfterSeconds())));

              default -> throw new CompletionException(new RuntimeException("Failed to check verification code: " + response.getError().getErrorType()));
            }
          } else {
            return response.getVerified() || response.getSessionMetadata().getVerified();
          }
        });
  }

  public CompletableFuture<Optional<RegistrationSession>> getSession(final byte[] sessionId,
      final Duration timeout) {
    return toCompletableFuture(stub.withDeadline(toDeadline(timeout)).getSessionMetadata(
        GetRegistrationSessionMetadataRequest.newBuilder()
            .setSessionId(ByteString.copyFrom(sessionId)).build()))
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case GET_REGISTRATION_SESSION_METADATA_ERROR_TYPE_NOT_FOUND -> {
                return Optional.empty();
              }
              default -> throw new RuntimeException("Failed to get session: " + response.getError().getErrorType());
            }
          }

          final String number = convertNumeralE164ToString(response.getSessionMetadata().getE164());
          return Optional.of(new RegistrationSession(number, response.getSessionMetadata().getVerified()));
        });
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
