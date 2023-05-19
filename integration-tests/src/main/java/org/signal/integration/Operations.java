/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.signal.integration.config.Config;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.RegistrationRequest;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;
import org.whispersystems.textsecuregcm.http.FaultTolerantHttpClient;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
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
    final byte[] registrationPassword = RandomUtils.nextBytes(32);
    final String accountPassword = Base64.getEncoder().encodeToString(RandomUtils.nextBytes(32));

    final TestUser user = TestUser.create(number, accountPassword, registrationPassword);
    final AccountAttributes accountAttributes = user.accountAttributes();

    INTEGRATION_TOOLS.populateRecoveryPassword(number, registrationPassword).join();

    // register account
    final RegistrationRequest registrationRequest = new RegistrationRequest(
        null, registrationPassword, accountAttributes, true,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    final AccountIdentityResponse registrationResponse = apiPost("/v1/registration", registrationRequest)
        .authorized(number, accountPassword)
        .executeExpectSuccess(AccountIdentityResponse.class);

    user.setAciUuid(registrationResponse.uuid());
    user.setPniUuid(registrationResponse.pni());

    // upload pre-key
    final TestUser.PreKeySetPublicView preKeySetPublicView = user.preKeys(Device.MASTER_ID, false);
    apiPut("/v2/keys", preKeySetPublicView)
        .authorized(user, Device.MASTER_ID)
        .executeExpectSuccess();

    return user;
  }

  public static TestUser newRegisteredUserAtomic(final String number) {
    final byte[] registrationPassword = RandomUtils.nextBytes(32);
    final String accountPassword = Base64.getEncoder().encodeToString(RandomUtils.nextBytes(32));

    final TestUser user = TestUser.create(number, accountPassword, registrationPassword);
    final AccountAttributes accountAttributes = user.accountAttributes();

    INTEGRATION_TOOLS.populateRecoveryPassword(number, registrationPassword).join();

    final ECKeyPair aciIdentityKeyPair = Curve.generateKeyPair();
    final ECKeyPair pniIdentityKeyPair = Curve.generateKeyPair();

    // register account
    final RegistrationRequest registrationRequest = new RegistrationRequest(null,
        registrationPassword,
        accountAttributes,
        true,
        Optional.of(aciIdentityKeyPair.getPublicKey().serialize()),
        Optional.of(pniIdentityKeyPair.getPublicKey().serialize()),
        Optional.of(generateSignedECPreKey(1, aciIdentityKeyPair)),
        Optional.of(generateSignedECPreKey(2, pniIdentityKeyPair)),
        Optional.of(generateSignedKEMPreKey(3, aciIdentityKeyPair)),
        Optional.of(generateSignedKEMPreKey(4, pniIdentityKeyPair)),
        Optional.empty(),
        Optional.empty());

    final AccountIdentityResponse registrationResponse = apiPost("/v1/registration", registrationRequest)
        .authorized(number, accountPassword)
        .executeExpectSuccess(AccountIdentityResponse.class);

    user.setAciUuid(registrationResponse.uuid());
    user.setPniUuid(registrationResponse.pni());

    return user;
  }

  public static void deleteUser(final TestUser user) {
    apiDelete("/v1/accounts/me").authorized(user).executeExpectSuccess();
  }

  public static String peekVerificationSessionPushChallenge(final String sessionId) {
    return INTEGRATION_TOOLS.peekVerificationSessionPushChallenge(sessionId).join()
        .orElseThrow(() -> new RuntimeException("push challenge not found for the verification session"));
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
      return authorized(user, Device.MASTER_ID);
    }

    public RequestBuilder authorized(final TestUser user, final long deviceId) {
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
          execute.getLeft() >= 200 && execute.getLeft() < 300,
          "Unexpected response code: %d",
          execute.getLeft());
      return execute;
    }

    public <T> T executeExpectSuccess(final Class<T> expectedType) {
      final Pair<Integer, T> execute = execute(expectedType);
      return requireNonNull(execute.getRight());
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
      return SystemMapper.yamlMapper().readValue(Resources.toByteArray(configFileUrl), Config.class);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static SignedPreKey generateSignedECPreKey(long id, final ECKeyPair identityKeyPair) {
    final byte[] pubKey = Curve.generateKeyPair().getPublicKey().serialize();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey);
    return new SignedPreKey(id, pubKey, sig);
  }

  private static SignedPreKey generateSignedKEMPreKey(long id, final ECKeyPair identityKeyPair) {
    final byte[] pubKey = KEMKeyPair.generate(KEMKeyType.KYBER_1024).getPublicKey().serialize();
    final byte[] sig = identityKeyPair.getPrivateKey().calculateSignature(pubKey);
    return new SignedPreKey(id, pubKey, sig);
  }
}
