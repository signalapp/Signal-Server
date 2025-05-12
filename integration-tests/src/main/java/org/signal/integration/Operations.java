/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import io.dropwizard.configuration.ConfigurationValidationException;
import io.dropwizard.jersey.validation.Validators;
import jakarta.validation.ConstraintViolation;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.signal.integration.config.Config;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.protocol.kem.KEMKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyType;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.HttpUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public final class Operations {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Config CONFIG = loadConfigFromClasspath("config.yml");

  private static final IntegrationTools INTEGRATION_TOOLS = IntegrationTools.create(CONFIG);

  private static final String USER_AGENT = "integration-test";

  private static final FaultTolerantHttpClient CLIENT = buildClient();


  private Operations() {
    // utility class
  }

  public static TestUser newRegisteredUser(final String number) {
    final byte[] registrationPassword = populateRandomRecoveryPassword(number);
    final String accountPassword = Base64.getEncoder().encodeToString(randomBytes(32));

    final TestUser user = TestUser.create(number, accountPassword, registrationPassword);
    final AccountAttributes accountAttributes = user.accountAttributes();

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    // register account
    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        registrationPassword,
        accountAttributes,
        true,
        new IdentityKey(aciIdentityKeyPair.getPublicKey()),
        new IdentityKey(pniIdentityKeyPair.getPublicKey()),
        generateSignedECPreKey(1, aciIdentityKeyPair),
        generateSignedECPreKey(2, pniIdentityKeyPair),
        generateSignedKEMPreKey(3, aciIdentityKeyPair),
        generateSignedKEMPreKey(4, pniIdentityKeyPair),
        Optional.empty(),
        Optional.empty());

    final AccountIdentityResponse registrationResponse = apiPost("/v1/registration", registrationRequest)
        .authorized(number, accountPassword)
        .executeExpectSuccess(AccountIdentityResponse.class);

    user.setAciUuid(registrationResponse.uuid());
    user.setPniUuid(registrationResponse.pni());

    return user;
  }

  public record PrescribedVerificationNumber(String number, String verificationCode) {}

  public static PrescribedVerificationNumber prescribedVerificationNumber() {
      return new PrescribedVerificationNumber(
          CONFIG.prescribedRegistrationNumber(),
          CONFIG.prescribedRegistrationCode());
  }

  public static void deleteUser(final TestUser user) {
    apiDelete("/v1/accounts/me").authorized(user).executeExpectSuccess();
  }

  public static String peekVerificationSessionPushChallenge(final String sessionId) {
    return INTEGRATION_TOOLS.peekVerificationSessionPushChallenge(sessionId).join()
        .orElseThrow(() -> new RuntimeException("push challenge not found for the verification session"));
  }

  public static byte[] populateRandomRecoveryPassword(final String number) {
    final byte[] recoveryPassword = randomBytes(32);
    INTEGRATION_TOOLS.populateRecoveryPassword(number, recoveryPassword).join();

    return recoveryPassword;
  }

  public static <T> T sendEmptyRequestAuthenticated(
      final String endpoint,
      final String method,
      final String username,
      final String password,
      final Class<T> outputType) {
    try {
      final HttpRequest request = HttpRequest.newBuilder()
          .uri(serverUri(endpoint, Collections.emptyList()))
          .method(method, HttpRequest.BodyPublishers.noBody())
          .header(HttpHeaders.AUTHORIZATION, HeaderUtils.basicAuthHeader(username, password))
          .header(HttpHeaders.CONTENT_TYPE, "application/json")
          .build();
      return CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
          .whenComplete((response, error) -> {
            if (error != null) {
              logger.error("request error", error);
              error.printStackTrace();
            } else {
              logger.info("response: {}", response.statusCode());
              System.out.println("response: " + response.statusCode() + ", " + response.body());
            }
          })
          .thenApply(response -> {
            try {
              return outputType.equals(Void.class)
                  ? null
                  : SystemMapper.jsonMapper().readValue(response.body(), outputType);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          })
          .get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static byte[] randomBytes(int numBytes) {
    final byte[] bytes = new byte[numBytes];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }

  public static RequestBuilder apiGet(final String endpoint) {
    return new RequestBuilder(HttpRequest.newBuilder().GET(), endpoint);
  }

  public static RequestBuilder apiDelete(final String endpoint) {
    return new RequestBuilder(HttpRequest.newBuilder().DELETE(), endpoint);
  }

  public static <R> RequestBuilder apiPost(final String endpoint, final R input) {
    return RequestBuilder.withJsonBody(endpoint, "POST", input);
  }

  public static <R> RequestBuilder apiPut(final String endpoint, final R input) {
    return RequestBuilder.withJsonBody(endpoint, "PUT", input);
  }

  public static <R> RequestBuilder apiPatch(final String endpoint, final R input) {
    return RequestBuilder.withJsonBody(endpoint, "PATCH", input);
  }

  private static URI serverUri(final String endpoint, final List<String> queryParams) {
    final String query = queryParams.isEmpty()
        ? StringUtils.EMPTY
        : "?" + String.join("&", queryParams);
    return URI.create("https://" + CONFIG.domain() + endpoint + query);
  }

  public static class RequestBuilder {

    private final HttpRequest.Builder builder;

    private final String endpoint;

    private final List<String> queryParams = new ArrayList<>();


    private RequestBuilder(final HttpRequest.Builder builder, final String endpoint) {
      this.builder = builder;
      this.endpoint = endpoint;
    }

    private static <R> RequestBuilder withJsonBody(final String endpoint, final String method, final R input) {
      try {
        final byte[] body = SystemMapper.jsonMapper().writeValueAsBytes(input);
        return new RequestBuilder(HttpRequest.newBuilder()
            .header(HttpHeaders.CONTENT_TYPE, "application/json")
            .method(method, HttpRequest.BodyPublishers.ofByteArray(body)), endpoint);
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public RequestBuilder authorized(final TestUser user) {
      return authorized(user, Device.PRIMARY_ID);
    }

    public RequestBuilder authorized(final TestUser user, final byte deviceId) {
      final String username = "%s.%d".formatted(user.aciUuid().toString(), deviceId);
      return authorized(username, user.accountPassword());
    }

    public RequestBuilder authorized(final String username, final String password) {
      builder.header(HttpHeaders.AUTHORIZATION, HeaderUtils.basicAuthHeader(username, password));
      return this;
    }

    public RequestBuilder queryParam(final String key, final String value) {
      queryParams.add("%s=%s".formatted(key, value));
      return this;
    }

    public RequestBuilder header(final String name, final String value) {
      builder.header(name, value);
      return this;
    }

    public Pair<Integer, Void> execute() {
      return execute(Void.class);
    }

    public Pair<Integer, Void> executeExpectSuccess() {
      final Pair<Integer, Void> execute = execute();
      Validate.isTrue(
          HttpUtils.isSuccessfulResponse(execute.getLeft()),
          "Unexpected response code: %d",
          execute.getLeft());
      return execute;
    }

    public <T> T executeExpectSuccess(final Class<T> expectedType) {
      final Pair<Integer, T> execute = execute(expectedType);
      Validate.isTrue(
          HttpUtils.isSuccessfulResponse(execute.getLeft()),
          "Unexpected response code: %d : %s",
          execute.getLeft(), execute.getRight());
      return requireNonNull(execute.getRight());
    }

    public void executeExpectStatusCode(final int expectedStatusCode) {
      final Pair<Integer, Void> execute = execute(Void.class);
      Validate.isTrue(
          execute.getLeft() == expectedStatusCode,
          "Unexpected response code: %d",
          execute.getLeft()
      );
    }

    public <T> Pair<Integer, T> execute(final Class<T> expectedType) {
      builder.uri(serverUri(endpoint, queryParams))
          .header(HttpHeaders.USER_AGENT, USER_AGENT);
      return CLIENT.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
          .whenComplete((response, error) -> {
            if (error != null) {
              logger.error("request error", error);
              error.printStackTrace();
            }
          })
          .thenApply(response -> {
            try {
              final T result = expectedType.equals(Void.class)
                  ? null
                  : SystemMapper.jsonMapper().readValue(response.body(), expectedType);
              return Pair.of(response.statusCode(), result);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          })
          .join();
    }
  }

  private static FaultTolerantHttpClient buildClient() {
    try {
      return FaultTolerantHttpClient.newBuilder()
          .withName("integration-test")
          .withExecutor(Executors.newFixedThreadPool(16))
          .withRetryExecutor(Executors.newSingleThreadScheduledExecutor())
          .withCircuitBreaker(new CircuitBreakerConfiguration())
          .withTrustedServerCertificates(CONFIG.rootCert())
          .build();
    } catch (final CertificateException e) {
      throw new RuntimeException(e);
    }
  }

  private static Config loadConfigFromClasspath(final String filename) {
    try {
      final URL configFileUrl = Resources.getResource(filename);
      final Config config = SystemMapper.yamlMapper().readValue(Resources.toByteArray(configFileUrl), Config.class);

      final Set<ConstraintViolation<Config>> constraintViolations = Validators.newValidator().validate(config);

      if (!constraintViolations.isEmpty()) {
        throw new ConfigurationValidationException(filename, constraintViolations);
      }

      return config;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ECSignedPreKey generateSignedECPreKey(final long id, final ECKeyPair identityKeyPair) {
    final ECPublicKey pubKey = Curve.generateKeyPair().getPublicKey();
    final byte[] signature = identityKeyPair.getPrivateKey().calculateSignature(pubKey.serialize());
    return new ECSignedPreKey(id, pubKey, signature);
  }

  public static KEMSignedPreKey generateSignedKEMPreKey(final long id, final ECKeyPair identityKeyPair) {
    final KEMPublicKey pubKey = KEMKeyPair.generate(KEMKeyType.KYBER_1024).getPublicKey();
    final byte[] signature = identityKeyPair.getPrivateKey().calculateSignature(pubKey.serialize());
    return new KEMSignedPreKey(id, pubKey, signature);
  }
}
