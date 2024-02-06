/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
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
class AuthEnablementRefreshRequirementProviderTest {

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
  private ClientPresenceManager clientPresenceManager;

  private AuthEnablementRefreshRequirementProvider provider;

  @BeforeEach
  void setup() {
    accountsManager = mock(AccountsManager.class);
    clientPresenceManager = mock(ClientPresenceManager.class);

    provider = new AuthEnablementRefreshRequirementProvider(accountsManager);

    final WebsocketRefreshRequestEventListener listener =
        new WebsocketRefreshRequestEventListener(clientPresenceManager, provider);

    when(applicationEventListener.onRequest(any())).thenReturn(listener);

    final UUID uuid = UUID.randomUUID();
    account.setUuid(uuid);
    account.addDevice(authenticatedDevice);
    IntStream.range(2, 4)
        .forEach(deviceId -> account.addDevice(DevicesHelper.createDevice((byte) deviceId)));

    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));

    account.getDevices()
        .forEach(device -> when(clientPresenceManager.isPresent(uuid, device.getId())).thenReturn(true));
  }

  @ParameterizedTest
  @MethodSource
  void testDeviceEnabledChanged(final Map<Byte, Boolean> initialEnabled, final Map<Byte, Boolean> finalEnabled) {
    assert initialEnabled.size() == finalEnabled.size();

    assert account.getPrimaryDevice().isEnabled();

    initialEnabled.forEach((deviceId, enabled) ->
        DevicesHelper.setEnabled(account.getDevice(deviceId).orElseThrow(), enabled));

    final Response response = resources.getJerseyTest()
        .target("/v1/test/account/devices/enabled")
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .post(Entity.entity(finalEnabled, MediaType.APPLICATION_JSON));

    assertEquals(200, response.getStatus());

    final boolean expectDisplacedPresence = !initialEnabled.equals(finalEnabled);

    assertAll(
        initialEnabled.keySet().stream()
            .map(deviceId -> () -> verify(clientPresenceManager, times(expectDisplacedPresence ? 1 : 0))
                .disconnectPresence(account.getUuid(), deviceId)));

    assertAll(
        finalEnabled.keySet().stream()
            .map(deviceId -> () -> verify(clientPresenceManager, times(expectDisplacedPresence ? 1 : 0))
                .disconnectPresence(account.getUuid(), deviceId)));
  }

  static Stream<Arguments> testDeviceEnabledChanged() {
    final byte deviceId2 = 2;
    final byte deviceId3 = 3;
    return Stream.of(
        Arguments.of(Map.of(deviceId2, false, deviceId3, false), Map.of(deviceId2, true, deviceId3, true)),
        Arguments.of(Map.of(deviceId2, true, deviceId3, true), Map.of(deviceId2, false, deviceId3, false)),
        Arguments.of(Map.of(deviceId2, true, deviceId3, true), Map.of(deviceId2, true, deviceId3, true)),
        Arguments.of(Map.of(deviceId2, false, deviceId3, true), Map.of(deviceId2, true, deviceId3, true)),
        Arguments.of(Map.of(deviceId2, true, deviceId3, false), Map.of(deviceId2, true, deviceId3, true))
    );
  }

  @Test
  void testDeviceAdded() {
    assert account.getPrimaryDevice().isEnabled();

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

    verify(clientPresenceManager).disconnectPresence(account.getUuid(), (byte) 1);
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), (byte) 2);
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), (byte) 3);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testDeviceRemoved(final int removedDeviceCount) {
    assert account.getPrimaryDevice().isEnabled();

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
        verify(clientPresenceManager).disconnectPresence(account.getUuid(), deviceId));

    verifyNoMoreInteractions(clientPresenceManager);
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
    @Path("/account/enabled/{enabled}")
    @ChangesDeviceEnabledState
    public String setAccountEnabled(@Auth TestPrincipal principal, @PathParam("enabled") final boolean enabled) {

      final Device device = principal.getAccount().getPrimaryDevice();

      DevicesHelper.setEnabled(device, enabled);

      assert device.isEnabled() == enabled;

      return String.format("Set account to %s", enabled);
    }

    @POST
    @Path("/account/devices/enabled")
    @ChangesDeviceEnabledState
    public String setEnabled(@Auth TestPrincipal principal, Map<Byte, Boolean> deviceIdsEnabled) {

      final StringBuilder response = new StringBuilder();

      for (Entry<Byte, Boolean> deviceIdEnabled : deviceIdsEnabled.entrySet()) {
        final Device device = principal.getAccount().getDevice(deviceIdEnabled.getKey()).orElseThrow();
        DevicesHelper.setEnabled(device, deviceIdEnabled.getValue());

        response.append(String.format("Set device enabled %s", deviceIdEnabled));
      }

      return response.toString();
    }

    @PUT
    @Path("/account/devices")
    @ChangesDeviceEnabledState
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
    @ChangesDeviceEnabledState
    public String removeDevices(@Auth TestPrincipal auth, @PathParam("deviceIds") String deviceIds) {

      Arrays.stream(deviceIds.split(","))
          .map(Byte::valueOf)
          .forEach(auth.getAccount()::removeDevice);

      return "Removed device(s) " + deviceIds;
    }

    @POST
    @Path("/account/disablePrimaryDeviceAndDeleteDevice/{deviceId}")
    @ChangesDeviceEnabledState
    public String disablePrimaryDeviceAndRemoveDevice(@Auth TestPrincipal auth, @PathParam("deviceId") byte deviceId) {

      DevicesHelper.setEnabled(auth.getAccount().getPrimaryDevice(), false);

      auth.getAccount().removeDevice(deviceId);

      return "Removed device " + deviceId;
    }
  }
}
