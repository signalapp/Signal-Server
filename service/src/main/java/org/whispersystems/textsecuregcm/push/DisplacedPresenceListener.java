package org.whispersystems.textsecuregcm.push;

/**
 * A displaced presence listener is notified when a specific client's presence has been displaced because the same
 * client opened a newer connection to the Signal service.
 */
@FunctionalInterface
public interface DisplacedPresenceListener {

    void handleDisplacement();
}
