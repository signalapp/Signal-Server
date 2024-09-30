/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.ProvisioningMessage;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ProvisioningManager;
import org.whispersystems.textsecuregcm.storage.PubSubProtos;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import org.whispersystems.websocket.session.WebSocketSessionContext;
import org.whispersystems.websocket.setup.WebSocketConnectListener;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

/**
 * A "provisioning WebSocket" provides a mechanism for sending a caller-defined provisioning message from the primary
 * device associated with a Signal account to a new device that is not yet associated with a Signal account. Generally,
 * the message contains key material and credentials the new device needs to associate itself with the primary device's
 * Signal account.
 * <p>
 * New devices initiate the provisioning process by opening a provisioning WebSocket. The server assigns the new device
 * a random, temporary "provisioning address," which it transmits via the newly-opened WebSocket. From there, the new
 * device generally displays the provisioning address (and a public key) as a QR code. After that, the primary device
 * will scan the QR code and send an encrypted provisioning message to the new device via
 * {@link org.whispersystems.textsecuregcm.controllers.ProvisioningController#sendProvisioningMessage(AuthenticatedDevice, String, ProvisioningMessage, String)}.
 * Once the server receives the message from the primary device, it sends the message to the new device via the open
 * WebSocket, then closes the WebSocket connection.
 */
public class ProvisioningConnectListener implements WebSocketConnectListener {

  private final ProvisioningManager provisioningManager;

  private final Map<ClientPlatform, AtomicInteger> openWebsocketsByClientPlatform;
  private final AtomicInteger openWebsocketsFromUnknownPlatforms;

  private static final String OPEN_WEBSOCKET_GAUGE_NAME = name(ProvisioningConnectListener.class, "openWebsockets");

  public ProvisioningConnectListener(final ProvisioningManager provisioningManager) {
    this.provisioningManager = provisioningManager;

    openWebsocketsByClientPlatform = new EnumMap<>(ClientPlatform.class);

    Arrays.stream(ClientPlatform.values())
        .forEach(clientPlatform -> openWebsocketsByClientPlatform.put(clientPlatform,
            Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME,
                Tags.of(UserAgentTagUtil.PLATFORM_TAG, clientPlatform.name().toLowerCase()),
                new AtomicInteger(0))));

    openWebsocketsFromUnknownPlatforms = Metrics.gauge(OPEN_WEBSOCKET_GAUGE_NAME,
        Tags.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized"),
        new AtomicInteger(0));
  }

  @Override
  public void onWebSocketConnect(WebSocketSessionContext context) {
    final String provisioningAddress = generateProvisioningAddress();
    context.addWebsocketClosedListener((context1, statusCode, reason) -> provisioningManager.removeListener(provisioningAddress));

    getOpenWebsocketCounter(context.getClient().getUserAgent()).incrementAndGet();

    context.addWebsocketClosedListener((context1, statusCode, reason) ->
        getOpenWebsocketCounter(context.getClient().getUserAgent()).decrementAndGet());

    provisioningManager.addListener(provisioningAddress, message -> {
      assert message.getType() == PubSubProtos.PubSubMessage.Type.DELIVER;

      final Optional<byte[]> body = Optional.of(message.getContent().toByteArray());

      context.getClient().sendRequest("PUT", "/v1/message", List.of(HeaderUtils.getTimestampHeader()), body)
          .whenComplete((ignored, throwable) -> context.getClient().close(1000, "Closed"));
    });

    context.getClient().sendRequest("PUT", "/v1/address", List.of(HeaderUtils.getTimestampHeader()),
        Optional.of(MessageProtos.ProvisioningAddress.newBuilder()
            .setAddress(provisioningAddress)
            .build().toByteArray()));
  }

  @VisibleForTesting
  public static String generateProvisioningAddress() {
    final byte[] provisioningAddress = new byte[16];
    new SecureRandom().nextBytes(provisioningAddress);

    return Base64.getUrlEncoder().encodeToString(provisioningAddress);
  }

  private AtomicInteger getOpenWebsocketCounter(final String userAgentString) {
    try {
      return openWebsocketsByClientPlatform.get(UserAgentUtil.parseUserAgentString(userAgentString).getPlatform());
    } catch (final UnrecognizedUserAgentException e) {
      return openWebsocketsFromUnknownPlatforms;
    }
  }
}
