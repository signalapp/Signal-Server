/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.auth.Auth;
import io.dropwizard.auth.PolymorphicAuthDynamicFeature;
import io.dropwizard.auth.PolymorphicAuthValueFactoryProvider;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.websocket.WebSocketResourceProvider;
import org.whispersystems.websocket.auth.WebsocketAuthValueFactoryProvider;
import org.whispersystems.websocket.logging.WebsocketRequestLog;
import org.whispersystems.websocket.messages.protobuf.ProtobufWebSocketMessageFactory;
import org.whispersystems.websocket.messages.protobuf.SubProtocol;
import org.whispersystems.websocket.session.WebSocketSessionContextValueFactoryProvider;

@ExtendWith(DropwizardExtensionsSupport.class)
class AuthEnablementRefreshRequirementProviderTest {

  private final ApplicationEventListener applicationEventListener = mock(ApplicationEventListener.class);

  private final Account account = new Account();
  private final Device authenticatedDevice = DevicesHelper.createDevice(1L);

  private final Supplier<Optional<TestPrincipal>> principalSupplier = () -> Optional.of(
      new TestPrincipal("test", account, authenticatedDevice));

  private final ResourceExtension resources = ResourceExtension.builder()
      .addProvider(
          new PolymorphicAuthDynamicFeature<>(ImmutableMap.of(
              TestPrincipal.class,
              new BasicCredentialAuthFilter.Builder<TestPrincipal>()
                  .setAuthenticator(c -> principalSupplier.get()).buildAuthFilter())))
      .addProvider(new PolymorphicAuthValueFactoryProvider.Binder<>(ImmutableSet.of(TestPrincipal.class)))
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
    LongStream.range(2, 4).forEach(deviceId -> account.addDevice(DevicesHelper.createDevice(deviceId)));

    when(accountsManager.getByAccountIdentifier(uuid)).thenReturn(Optional.of(account));

    account.getDevices()
        .forEach(device -> when(clientPresenceManager.isPresent(uuid, device.getId())).thenReturn(true));
  }

  @Test
  void testBuildDevicesEnabled() {

    final long disabledDeviceId = 3L;

    final Account account = mock(Account.class);

    final List<Device> devices = new ArrayList<>();
    when(account.getDevices()).thenReturn(devices);

    LongStream.range(1, 5)
        .forEach(id -> {
          final Device device = mock(Device.class);
          when(device.getId()).thenReturn(id);
          when(device.isEnabled()).thenReturn(id != disabledDeviceId);
          devices.add(device);
        });

    final Map<Long, Boolean> devicesEnabled = AuthEnablementRefreshRequirementProvider.buildDevicesEnabledMap(account);

    assertEquals(4, devicesEnabled.size());

    assertAll(devicesEnabled.entrySet().stream()
        .map(deviceAndEnabled -> () -> {
          if (deviceAndEnabled.getKey().equals(disabledDeviceId)) {
            assertFalse(deviceAndEnabled.getValue());
          } else {
            assertTrue(deviceAndEnabled.getValue());
          }
        }));
  }

  @ParameterizedTest
  @MethodSource
  void testDeviceEnabledChanged(final Map<Long, Boolean> initialEnabled, final Map<Long, Boolean> finalEnabled) {
    assert initialEnabled.size() == finalEnabled.size();

    assert account.getMasterDevice().orElseThrow().isEnabled();

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
    return Stream.of(
        Arguments.of(Map.of(1L, false, 2L, false), Map.of(1L, true, 2L, false)),
        Arguments.of(Map.of(2L, false, 3L, false), Map.of(2L, true, 3L, true)),
        Arguments.of(Map.of(2L, true, 3L, true), Map.of(2L, false, 3L, false)),
        Arguments.of(Map.of(2L, true, 3L, true), Map.of(2L, true, 3L, true)),
        Arguments.of(Map.of(2L, false, 3L, true), Map.of(2L, true, 3L, true)),
        Arguments.of(Map.of(2L, true, 3L, false), Map.of(2L, true, 3L, true))
    );
  }

  @Test
  void testDeviceAdded() {
    assert account.getMasterDevice().orElseThrow().isEnabled();

    final int initialDeviceCount = account.getDevices().size();

    final List<String> addedDeviceNames = List.of("newDevice1", "newDevice2");
    final Response response = resources.getJerseyTest()
        .target("/v1/test/account/devices")
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .put(Entity.entity(addedDeviceNames, MediaType.APPLICATION_JSON_PATCH_JSON));

    assertEquals(200, response.getStatus());

    assertEquals(initialDeviceCount + addedDeviceNames.size(), account.getDevices().size());

    verify(clientPresenceManager).disconnectPresence(account.getUuid(), 1);
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), 2);
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), 3);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void testDeviceRemoved(final int removedDeviceCount) {
    assert account.getMasterDevice().orElseThrow().isEnabled();

    final List<Long> initialDeviceIds = account.getDevices().stream().map(Device::getId).collect(Collectors.toList());

    final List<Long> deletedDeviceIds = account.getDevices().stream()
        .map(Device::getId)
        .filter(deviceId -> deviceId != 1L)
        .limit(removedDeviceCount)
        .collect(Collectors.toList());

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
  void testMasterDeviceDisabledAndDeviceRemoved() {
    assert account.getMasterDevice().orElseThrow().isEnabled();

    final Set<Long> initialDeviceIds = account.getDevices().stream().map(Device::getId).collect(Collectors.toSet());

    final long deletedDeviceId = 2L;
    assertTrue(initialDeviceIds.remove(deletedDeviceId));

    final Response response = resources.getJerseyTest()
        .target("/v1/test/account/disableMasterDeviceAndDeleteDevice/" + deletedDeviceId)
        .request()
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8)))
        .post(Entity.entity("", MediaType.TEXT_PLAIN));

    assertEquals(200, response.getStatus());

    assertTrue(account.getDevice(deletedDeviceId).isEmpty());

    initialDeviceIds.forEach(deviceId -> verify(clientPresenceManager).disconnectPresence(account.getUuid(), deviceId));
    verify(clientPresenceManager).disconnectPresence(account.getUuid(), deletedDeviceId);

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

      provider = new WebSocketResourceProvider<>("127.0.0.1", applicationHandler,
          requestLog, new TestPrincipal("test", account, authenticatedDevice), new ProtobufWebSocketMessageFactory(),
          Optional.empty(), 30000);

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
        throws InvalidProtocolBufferException {
      ArgumentCaptor<ByteBuffer> responseBytesCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
      verify(remoteEndpoint).sendBytesByFuture(responseBytesCaptor.capture());

      return SubProtocol.WebSocketMessage.parseFrom(responseBytesCaptor.getValue().array()).getResponse();
    }
  }

  public static class TestPrincipal implements Principal, AccountAndAuthenticatedDeviceHolder {

    private final String name;
    private final Account account;
    private final Device device;

    private TestPrincipal(String name, final Account account, final Device device) {
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

      final Device device = principal.getAccount().getMasterDevice().orElseThrow();

      DevicesHelper.setEnabled(device, enabled);

      assert device.isEnabled() == enabled;

      return String.format("Set account to %s", enabled);
    }

    @POST
    @Path("/account/devices/enabled")
    @ChangesDeviceEnabledState
    public String setEnabled(@Auth TestPrincipal principal, Map<Long, Boolean> deviceIdsEnabled) {

      final StringBuilder response = new StringBuilder();

      for (Entry<Long, Boolean> deviceIdEnabled : deviceIdsEnabled.entrySet()) {
        final Device device = principal.getAccount().getDevice(deviceIdEnabled.getKey()).orElseThrow();
        DevicesHelper.setEnabled(device, deviceIdEnabled.getValue());

        response.append(String.format("Set device enabled %s", deviceIdEnabled));
      }

      return response.toString();
    }

    @PUT
    @Path("/account/devices")
    @ChangesDeviceEnabledState
    public String addDevices(@Auth TestPrincipal auth, List<String> deviceNames) {

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
          .map(Long::valueOf)
          .forEach(auth.getAccount()::removeDevice);

      return "Removed device(s) " + deviceIds;
    }

    @POST
    @Path("/account/disableMasterDeviceAndDeleteDevice/{deviceId}")
    @ChangesDeviceEnabledState
    public String disableMasterDeviceAndRemoveDevice(@Auth TestPrincipal auth, @PathParam("deviceId") long deviceId) {

      DevicesHelper.setEnabled(auth.getAccount().getMasterDevice().orElseThrow(), false);

      auth.getAccount().removeDevice(deviceId);

      return "Removed device " + deviceId;
    }
  }
}
