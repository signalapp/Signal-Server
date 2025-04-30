/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import javax.crypto.ShortBufferException;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.grpc.net.client.NoiseClientHandshakeHelper;


public class NoiseHandshakeHelperTest {

  @ParameterizedTest
  @EnumSource(HandshakePattern.class)
  void testWithPayloads(final HandshakePattern pattern) throws ShortBufferException, NoiseHandshakeException {
    doHandshake(pattern, "ping".getBytes(StandardCharsets.UTF_8), "pong".getBytes(StandardCharsets.UTF_8));
  }

  @ParameterizedTest
  @EnumSource(HandshakePattern.class)
  void testWithRequestPayload(final HandshakePattern pattern) throws ShortBufferException, NoiseHandshakeException {
    doHandshake(pattern, "ping".getBytes(StandardCharsets.UTF_8), new byte[0]);
  }

  @ParameterizedTest
  @EnumSource(HandshakePattern.class)
  void testWithoutPayloads(final HandshakePattern pattern) throws ShortBufferException, NoiseHandshakeException {
    doHandshake(pattern, new byte[0], new byte[0]);
  }

  void doHandshake(final HandshakePattern pattern, final byte[] requestPayload, final byte[] responsePayload) throws ShortBufferException, NoiseHandshakeException {
    final ECKeyPair serverKeyPair = Curve.generateKeyPair();
    final ECKeyPair clientKeyPair = Curve.generateKeyPair();

    NoiseHandshakeHelper serverHelper = new NoiseHandshakeHelper(pattern, serverKeyPair);
    NoiseClientHandshakeHelper clientHelper = switch (pattern) {
      case IK -> NoiseClientHandshakeHelper.IK(serverKeyPair.getPublicKey(), clientKeyPair);
      case NK -> NoiseClientHandshakeHelper.NK(serverKeyPair.getPublicKey());
    };

    final byte[] initiate = clientHelper.write(requestPayload);
    final ByteBuf actualRequestPayload = serverHelper.read(initiate);
    assertThat(ByteBufUtil.getBytes(actualRequestPayload)).isEqualTo(requestPayload);

    assertThat(serverHelper.getHandshakeState().getAction()).isEqualTo(HandshakeState.WRITE_MESSAGE);

    final byte[] respond = serverHelper.write(responsePayload);
    byte[] actualResponsePayload = clientHelper.read(respond);
    assertThat(actualResponsePayload).isEqualTo(responsePayload);

    assertThat(serverHelper.getHandshakeState().getAction()).isEqualTo(HandshakeState.SPLIT);
    assertThatNoException().isThrownBy(() -> serverHelper.getHandshakeState().split());
    assertThatNoException().isThrownBy(() -> clientHelper.split());
  }

}
