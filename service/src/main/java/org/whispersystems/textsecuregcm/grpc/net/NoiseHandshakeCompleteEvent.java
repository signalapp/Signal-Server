package org.whispersystems.textsecuregcm.grpc.net;

import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import java.util.Optional;

/**
 * An event that indicates that a Noise handshake has completed, possibly authenticating a caller in the process.
 *
 * @param authenticatedDevice the device authenticated as part of the handshake, or empty if the handshake was not of a
 * type that performs authentication
 */
record NoiseHandshakeCompleteEvent(Optional<AuthenticatedDevice> authenticatedDevice) {
}
