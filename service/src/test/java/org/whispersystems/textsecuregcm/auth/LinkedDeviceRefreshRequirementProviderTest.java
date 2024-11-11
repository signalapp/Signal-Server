/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.ReusableAuth;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.auth.PrincipalSupplier;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.SubProtocol;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class LinkedDeviceRefreshRequirementProviderTest {

  private final ApplicationEventListener applicationEventListener = mock(ApplicationEventListener.class);

  private final Account account = new Account();
  private final Device authenticatedDevice = DevicesHelper.createDevice(Device.PRIMARY_ID);

  private final Supplier<Optional<TestPrincipal>> principalSupplier = () -> Optional.of(
      new TestPrincipal("test", account, authenticatedDevice));

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(new AuthDynamicFeature(new BasicCredentialAuthFilter.Builder<TestPrincipal>()
          .setAuthenticator(c -> principalSupplier.get()).buildAuthFilter()))
      .addProvider(new AuthValueFactoryProvider.Binder<>(TestPrincipal.class))
      .addProvider(applicationEventListener)
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new TestResource())
      .build();

  private AccountsManager accountsManager;
  private DisconnectionRequestManager disconnectionRequestManager;

  private LinkedDeviceRefreshRequirementProvider provider;

  @BeforeEach
  void setup() {
    accountsManager = mock(AccountsManager.class);
    disconnectionRequestManager = mock(DisconnectionRequestManager.class);

    provider = new LinkedDeviceRefreshRequirementProvider(accountsManager);

    final WebsocketRefreshRequestEventListener listener =
        new WebsocketRefreshRequestEventListener(disconnectionRequestManager, provider);

    when(applicationEventListener.onRequest(any())).thenReturn(listener);

    final UUID uuid = UUID.randomUUID();
    account.setUuid(uuid);
    account.addDevice(authenticatedDevice);
    IntStream.range(2, 4)
        .forEach(deviceId -> account.addDevice(DevicesHelper.createDevice((byte) deviceId)));

    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));
  }

  @Test
  void testDeviceAdded() {
    final int initialDeviceCount = account.getDevices().size();

    final List<String> addedDeviceNames = List.of(
        Base64.getEncoder().encodeToString("newDevice1".getBytes(StandardCharsets.UTF_8)),
        Base64.getEncoder().encodeToString("newDevice2".getBytes(StandardCharsets.UTF_8)));

    final Response response = resources.getJerseyTest()
        .target("/v1/test/account/devices")
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .put(Entity.entity(addedDeviceNames, MediaType.APPLICATION_JSON_PATCH_JSON));

    assertEquals(200, response.getStatus());

    assertEquals(initialDeviceCount + addedDeviceNames.size(), account.getDevices().size());

    verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of((byte) 1));
    verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of((byte) 2));
    verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of((byte) 3));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testDeviceRemoved(final int removedDeviceCount) {
    final List<Byte> initialDeviceIds = account.getDevices().stream().map(Device::getId).toList();

    final List<Byte> deletedDeviceIds = account.getDevices().stream()
        .map(Device::getId)
        .filter(deviceId -> deviceId != Device.PRIMARY_ID)
        .limit(removedDeviceCount)
        .toList();

    assert deletedDeviceIds.size() == removedDeviceCount;

    final String deletedDeviceIdsParam = deletedDeviceIds.stream().map(String::valueOf)
        .collect(Collectors.joining(","));

    final Response response = resources.getJerseyTest()
        .target("/v1/test/account/devices/" + deletedDeviceIdsParam)
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .delete();

    assertEquals(200, response.getStatus());

    initialDeviceIds.forEach(deviceId ->
        verify(disconnectionRequestManager).requestDisconnection(account.getUuid(), List.of(deviceId)));

    verifyNoMoreInteractions(disconnectionRequestManager);
  }

  @Test
  void testOnEvent() {
    Response response = resources.getJerseyTest()
        .target("/v1/test/hello")
        .request()
        // no authorization required
        .get();

    assertEquals(200, response.getStatus());

    response = resources.getJerseyTest()
        .target("/v1/test/authorized")
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .get();

    assertEquals(200, response.getStatus());

    verify(accountsManager, never()).getByAccountIdentifier(any(UUID.class));
  }

  @Nested
  class WebSocket {

    private WebSocketResourceProvider<TestPrincipal> provider;
    private RemoteEndpoint remoteEndpoint;

    @BeforeEach
    void setup() {
      ResourceConfig resourceConfig = new DropwizardResourceConfig();
      resourceConfig.register(applicationEventListener);
      resourceConfig.register(new TestResource());
      resourceConfig.register(new WebSocketSessionContextValueFactoryProvider.Binder());
      resourceConfig.register(new WebsocketAuthValueFactoryProvider.Binder<>(TestPrincipal.class));
      resourceConfig.register(new JacksonMessageBodyProvider(SystemMapper.jsonMapper()));

      ApplicationHandler applicationHandler = new ApplicationHandler(resourceConfig);
      WebsocketRequestLog requestLog = mock(WebsocketRequestLog.class);

      provider = new WebSocketResourceProvider<>("127.0.0.1", RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME,
          applicationHandler, requestLog, TestPrincipal.reusableAuth("test", account, authenticatedDevice),
          new ProtobufWebSocketMessageFactory(), Optional.empty(), Duration.ofMillis(30000));

      remoteEndpoint = mock(RemoteEndpoint.class);
      Session session = mock(Session.class);
      UpgradeRequest request = mock(UpgradeRequest.class);

      when(session.getRemote()).thenReturn(remoteEndpoint);
      when(session.getUpgradeRequest()).thenReturn(request);

      provider.onWebSocketConnect(session);
    }

    @Test
    void testOnEvent() throws Exception {

      byte[] message = new ProtobufWebSocketMessageFactory().createRequest(Optional.of(111L), "GET", "/v1/test/hello",
          new LinkedList<>(), Optional.empty()).toByteArray();

      provider.onWebSocketBinary(message, 0, message.length);

      final SubProtocol.WebSocketResponseMessage response = verifyAndGetResponse(remoteEndpoint);

      assertEquals(200, response.getStatus());
    }

    private SubProtocol.WebSocketResponseMessage verifyAndGetResponse(final RemoteEndpoint remoteEndpoint)
        throws IOException {
      ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
      verify(remoteEndpoint).sendBytes(responseBytesCaptor.capture(), any(WriteCallback.class));

      return SubProtocol.WebSocketMessage.parseFrom(responseBytesCaptor.getValue().array()).getResponse();
    }
  }

  public static class TestPrincipal implements Principal, AccountAndAuthenticatedDeviceHolder {

    private final String name;
    private final Account account;
    private final Device device;

    private TestPrincipal(final String name, final Account account, final Device device) {
      this.name = name;
      this.account = account;
      this.device = device;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Account getAccount() {
      return account;
    }

    @Override
    public Device getAuthenticatedDevice() {
      return device;
    }

    public static ReusableAuth<TestPrincipal> reusableAuth(final String name, final Account account, final Device device) {
      return ReusableAuth.authenticated(new TestPrincipal(name, account, device), PrincipalSupplier.forImmutablePrincipal());
    }

  }

  @Path("/v1/test")
  public static class TestResource {

    @GET
    @Path("/hello")
    public String testGetHello() {
      return "Hello!";
    }

    @GET
    @Path("/authorized")
    public String testAuth(@Auth TestPrincipal principal) {
      return "You’re in!";
    }

    @PUT
    @Path("/account/devices")
    @ChangesLinkedDevices
    public String addDevices(@Auth TestPrincipal auth, List<byte[]> deviceNames) {

      deviceNames.forEach(name -> {
        final Device device = DevicesHelper.createDevice(auth.getAccount().getNextDeviceId());
        auth.getAccount().addDevice(device);

        device.setName(name);
      });

      return "Added devices " + deviceNames;
    }

    @DELETE
    @Path("/account/devices/{deviceIds}")
    @ChangesLinkedDevices
    public String removeDevices(@Auth TestPrincipal auth, @PathParam("deviceIds") String deviceIds) {

      Arrays.stream(deviceIds.split(","))
          .map(Byte::valueOf)
          .forEach(auth.getAccount()::removeDevice);

      return "Removed device(s) " + deviceIds;
    }
  }
}
