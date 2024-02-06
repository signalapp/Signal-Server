/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoServiceGrpc;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.grpc.EchoServiceImpl;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.Pair;

class BasicCredentialAuthenticationInterceptorTest {

  private Server server;
  private ManagedChannel managedChannel;

  private AccountAuthenticator accountAuthenticator;


  @BeforeEach
  void setUp() throws IOException {
    accountAuthenticator = mock(AccountAuthenticator.class);

    final BasicCredentialAuthenticationInterceptor authenticationInterceptor =
        new BasicCredentialAuthenticationInterceptor(accountAuthenticator);

    final String serverName = InProcessServerBuilder.generateName();

    server = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .intercept(authenticationInterceptor)
        .addService(new EchoServiceImpl())
        .build()
        .start();

    managedChannel = InProcessChannelBuilder.forName(serverName)
        .directExecutor()
        .build();
  }

  @AfterEach
  void tearDown() {
    managedChannel.shutdown();
    server.shutdown();
  }

  @ParameterizedTest
  @MethodSource
  void interceptCall(final Metadata headers, final boolean acceptCredentials, final boolean expectAuthentication) {
    if (acceptCredentials) {
      final Account account = mock(Account.class);
      when(account.getUuid()).thenReturn(UUID.randomUUID());

      final Device device = mock(Device.class);
      when(device.getId()).thenReturn(Device.PRIMARY_ID);

      when(accountAuthenticator.authenticate(any()))
          .thenReturn(Optional.of(new AuthenticatedAccount(account, device)));
    } else {
      when(accountAuthenticator.authenticate(any()))
          .thenReturn(Optional.empty());
    }

    final EchoServiceGrpc.EchoServiceBlockingStub stub = EchoServiceGrpc.newBlockingStub(managedChannel)
        .withCallCredentials(new CallCredentials() {
          @Override
          public void applyRequestMetadata(final RequestInfo requestInfo, final Executor appExecutor, final MetadataApplier applier) {
            applier.apply(headers);
          }

          @Override
          public void thisUsesUnstableApi() {
          }
        });

    if (expectAuthentication) {
      assertDoesNotThrow(() -> stub.echo(EchoRequest.newBuilder().build()));
    } else {
      final StatusRuntimeException exception =
          assertThrows(StatusRuntimeException.class, () -> stub.echo(EchoRequest.newBuilder().build()));

      assertEquals(Status.UNAUTHENTICATED.getCode(), exception.getStatus().getCode());
    }
  }

  private static Stream<Arguments> interceptCall() {
    final Metadata malformedCredentialHeaders = new Metadata();
    malformedCredentialHeaders.put(BasicCredentialAuthenticationInterceptor.BASIC_CREDENTIALS, "Incorrect");

    final Metadata structurallyValidCredentialHeaders = new Metadata();
    structurallyValidCredentialHeaders.put(
        BasicCredentialAuthenticationInterceptor.BASIC_CREDENTIALS,
        HeaderUtils.basicAuthHeader(UUID.randomUUID().toString(), RandomStringUtils.randomAlphanumeric(16))
    );

    return Stream.of(
        Arguments.of(new Metadata(), true, false),
        Arguments.of(malformedCredentialHeaders, true, false),
        Arguments.of(structurallyValidCredentialHeaders, false, false),
        Arguments.of(structurallyValidCredentialHeaders, true, true)
    );
  }
}
