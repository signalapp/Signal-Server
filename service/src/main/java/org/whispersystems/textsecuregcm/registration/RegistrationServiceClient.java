package org.whispersystems.textsecuregcm.registration;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.google.protobuf.ByteString;
import io.dropwizard.lifecycle.Managed;
import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Deadline;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.signal.registration.rpc.CheckVerificationCodeRequest;
import org.signal.registration.rpc.CreateRegistrationSessionRequest;
import org.signal.registration.rpc.GetRegistrationSessionMetadataRequest;
import org.signal.registration.rpc.RegistrationServiceGrpc;
import org.signal.registration.rpc.RegistrationSessionMetadata;
import org.signal.registration.rpc.SendVerificationCodeRequest;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.controllers.VerificationSessionRateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.RegistrationServiceSession;
import org.whispersystems.textsecuregcm.util.CompletableFutureUtil;

public class RegistrationServiceClient implements Managed {

  private static final Base64.Encoder BASE64_UNPADDED_ENCODER = Base64.getEncoder().withoutPadding();

  private final ManagedChannel channel;
  private final RegistrationServiceGrpc.RegistrationServiceFutureStub stub;
  private final Executor callbackExecutor;
  private final byte[] collationKeySalt;

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
      final CallCredentials callCredentials,
      final String caCertificatePem,
      final byte[] collationKeySalt,
      final Executor callbackExecutor) throws IOException {

    try (final ByteArrayInputStream certificateInputStream = new ByteArrayInputStream(caCertificatePem.getBytes(StandardCharsets.UTF_8))) {
      final ChannelCredentials tlsChannelCredentials = TlsChannelCredentials.newBuilder()
          .trustManager(certificateInputStream)
          .build();

      this.channel = Grpc.newChannelBuilderForAddress(host, port, tlsChannelCredentials)
          .idleTimeout(1, TimeUnit.MINUTES)
          .build();
    }

    this.stub = RegistrationServiceGrpc.newFutureStub(channel).withCallCredentials(callCredentials);
    this.collationKeySalt = collationKeySalt;
    this.callbackExecutor = callbackExecutor;

    // Fail fast: reject bad keys
    try {
      getInitializedMac(collationKeySalt);
    } catch (final InvalidKeyException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public CompletableFuture<RegistrationServiceSession> createRegistrationSession(
      final Phonenumber.PhoneNumber phoneNumber, final String sourceHost, final boolean accountExistsWithPhoneNumber, final Duration timeout) {

    final long e164 = Long.parseLong(
        PhoneNumberUtil.getInstance().format(phoneNumber, PhoneNumberUtil.PhoneNumberFormat.E164).substring(1));
    final String rateLimitCollationKey = hmac(sourceHost);

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .createSession(CreateRegistrationSessionRequest.newBuilder()
            .setE164(e164)
            .setAccountExistsWithE164(accountExistsWithPhoneNumber)
            .setRateLimitCollationKey(rateLimitCollationKey)
            .build()), callbackExecutor)
        .thenApply(response -> switch (response.getResponseCase()) {
          case SESSION_METADATA -> buildSessionResponseFromMetadata(response.getSessionMetadata());

          case ERROR -> {
            switch (response.getError().getErrorType()) {
              case CREATE_REGISTRATION_SESSION_ERROR_TYPE_RATE_LIMITED -> throw new CompletionException(
                  new RateLimitExceededException(response.getError().getMayRetry()
                      ? Duration.ofSeconds(response.getError().getRetryAfterSeconds())
                      : null
                  ));
              case CREATE_REGISTRATION_SESSION_ERROR_TYPE_ILLEGAL_PHONE_NUMBER -> throw new IllegalArgumentException();
              default -> throw new RuntimeException(
                  "Unrecognized error type from registration service: " + response.getError().getErrorType());
            }
          }

          case RESPONSE_NOT_SET -> throw new RuntimeException("No response from registration service");
        });
  }

  public CompletableFuture<RegistrationServiceSession> sendVerificationCode(final byte[] sessionId,
      final MessageTransport messageTransport,
      final ClientType clientType,
      @Nullable final String acceptLanguage,
      @Nullable final String senderOverride,
      final Duration timeout) {

    final SendVerificationCodeRequest.Builder requestBuilder = SendVerificationCodeRequest.newBuilder()
        .setSessionId(ByteString.copyFrom(sessionId))
        .setTransport(getRpcMessageTransport(messageTransport))
        .setClientType(getRpcClientType(clientType));

    if (StringUtils.isNotBlank(acceptLanguage)) {
      requestBuilder.setAcceptLanguage(acceptLanguage);
    }

    if (StringUtils.isNotBlank(senderOverride)) {
      requestBuilder.setSenderName(senderOverride);
    }

    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .sendVerificationCode(requestBuilder.build()), callbackExecutor)
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case SEND_VERIFICATION_CODE_ERROR_TYPE_RATE_LIMITED -> throw new CompletionException(
                  new VerificationSessionRateLimitExceededException(
                      buildSessionResponseFromMetadata(response.getSessionMetadata()),
                      response.getError().getMayRetry()
                          ? Duration.ofSeconds(response.getError().getRetryAfterSeconds())
                          : null,
                      true));

              case SEND_VERIFICATION_CODE_ERROR_TYPE_SESSION_NOT_FOUND -> throw new CompletionException(
                  new RegistrationServiceException(null));

              case SEND_VERIFICATION_CODE_ERROR_TYPE_SESSION_ALREADY_VERIFIED -> throw new CompletionException(
                  new RegistrationServiceException(buildSessionResponseFromMetadata(response.getSessionMetadata())));

              case SEND_VERIFICATION_CODE_ERROR_TYPE_SENDER_REJECTED -> throw new CompletionException(
                  RegistrationServiceSenderException.rejected(response.getError().getMayRetry()));
              case SEND_VERIFICATION_CODE_ERROR_TYPE_SUSPECTED_FRAUD ->
                  throw new CompletionException(new RegistrationFraudException(
                      RegistrationServiceSenderException.rejected(response.getError().getMayRetry())));
              case SEND_VERIFICATION_CODE_ERROR_TYPE_SENDER_ILLEGAL_ARGUMENT -> throw new CompletionException(
                  RegistrationServiceSenderException.illegalArgument(response.getError().getMayRetry()));
              case SEND_VERIFICATION_CODE_ERROR_TYPE_UNSPECIFIED -> throw new CompletionException(
                  RegistrationServiceSenderException.unknown(response.getError().getMayRetry()));
              case SEND_VERIFICATION_CODE_ERROR_TYPE_TRANSPORT_NOT_ALLOWED -> throw new CompletionException(
                  new TransportNotAllowedException(buildSessionResponseFromMetadata(response.getSessionMetadata())));

              default -> throw new CompletionException(
                  new RuntimeException("Failed to send verification code: " + response.getError().getErrorType()));
            }
          } else {
            return buildSessionResponseFromMetadata(response.getSessionMetadata());
          }
        });
  }

  public CompletableFuture<RegistrationServiceSession> checkVerificationCode(final byte[] sessionId,
      final String verificationCode,
      final Duration timeout) {
    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout))
        .checkVerificationCode(CheckVerificationCodeRequest.newBuilder()
            .setSessionId(ByteString.copyFrom(sessionId))
            .setVerificationCode(verificationCode)
            .build()), callbackExecutor)
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case CHECK_VERIFICATION_CODE_ERROR_TYPE_RATE_LIMITED -> throw new CompletionException(
                  new VerificationSessionRateLimitExceededException(
                      buildSessionResponseFromMetadata(response.getSessionMetadata()),
                      response.getError().getMayRetry()
                          ? Duration.ofSeconds(response.getError().getRetryAfterSeconds())
                          : null,
                      true));

              case CHECK_VERIFICATION_CODE_ERROR_TYPE_NO_CODE_SENT, CHECK_VERIFICATION_CODE_ERROR_TYPE_ATTEMPT_EXPIRED ->
                  throw new CompletionException(
                      new RegistrationServiceException(buildSessionResponseFromMetadata(response.getSessionMetadata()))
                  );

              case CHECK_VERIFICATION_CODE_ERROR_TYPE_SESSION_NOT_FOUND -> throw new CompletionException(
                  new RegistrationServiceException(null)
              );

              default -> throw new CompletionException(
                  new RuntimeException("Failed to check verification code: " + response.getError().getErrorType()));
            }
          } else {
            return buildSessionResponseFromMetadata(response.getSessionMetadata());
          }
        });
  }

  public CompletableFuture<Optional<RegistrationServiceSession>> getSession(final byte[] sessionId,
      final Duration timeout) {
    return CompletableFutureUtil.toCompletableFuture(stub.withDeadline(toDeadline(timeout)).getSessionMetadata(
        GetRegistrationSessionMetadataRequest.newBuilder()
            .setSessionId(ByteString.copyFrom(sessionId)).build()), callbackExecutor)
        .thenApply(response -> {
          if (response.hasError()) {
            switch (response.getError().getErrorType()) {
              case GET_REGISTRATION_SESSION_METADATA_ERROR_TYPE_NOT_FOUND -> {
                return Optional.empty();
              }
              default -> throw new RuntimeException("Failed to get session: " + response.getError().getErrorType());
            }
          }

          return Optional.of(buildSessionResponseFromMetadata(response.getSessionMetadata()));
        });
  }

  private static RegistrationServiceSession buildSessionResponseFromMetadata(
      final RegistrationSessionMetadata sessionMetadata) {
    return new RegistrationServiceSession(sessionMetadata.getSessionId().toByteArray(),
        convertNumeralE164ToString(sessionMetadata.getE164()), sessionMetadata);
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

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
    if (channel != null) {
      channel.shutdown();
    }
  }

  private String hmac(String sourceHost) {
      final Mac hmacSha256 = getInitializedMac();
      hmacSha256.update(sourceHost.getBytes(StandardCharsets.UTF_8));

      return BASE64_UNPADDED_ENCODER.encodeToString(hmacSha256.doFinal());
  }

  private Mac getInitializedMac() {
    try {
      return getInitializedMac(collationKeySalt);
    } catch (final InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }


  private static Mac getInitializedMac(byte[] key) throws InvalidKeyException {
    try {
      final Mac hmacSha256 = Mac.getInstance("HmacSHA256");
      hmacSha256.init(new SecretKeySpec(key, "HmacSHA256"));
      return hmacSha256;
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
