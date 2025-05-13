package org.whispersystems.textsecuregcm.grpc.net;

import java.net.InetAddress;
import java.util.Optional;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;

/**
 * An event that indicates that an identity of a noise handshake initiator has been determined. If the initiator is
 * connecting anonymously, the identity is empty, otherwise it will be present and already authenticated.
 *
 * @param authenticatedDevice the device authenticated as part of the handshake, or empty if the handshake was not of a
 *                            type that performs authentication
 * @param remoteAddress       the remote address of the connecting client
 * @param userAgent           the client supplied userAgent
 * @param acceptLanguage      the client supplied acceptLanguage
 */
public record NoiseIdentityDeterminedEvent(
    Optional<AuthenticatedDevice> authenticatedDevice,
    InetAddress remoteAddress,
    String userAgent,
    String acceptLanguage) {}
