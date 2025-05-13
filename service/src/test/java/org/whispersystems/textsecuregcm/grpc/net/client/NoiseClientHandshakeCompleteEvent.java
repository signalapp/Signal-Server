/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.client;

import org.whispersystems.textsecuregcm.grpc.net.NoiseTunnelProtos;

import java.util.Optional;

/**
 * A netty user event that indicates that the noise handshake finished successfully.
 *
 * @param fastResponse A response if the client included a request to send in the initiate handshake message payload and
 *                     the server included a payload in the handshake response.
 */
public record NoiseClientHandshakeCompleteEvent(NoiseTunnelProtos.HandshakeResponse handshakeResponse) {}
