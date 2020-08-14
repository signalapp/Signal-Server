package org.whispersystems.textsecuregcm.push;

import java.util.UUID;

public interface ClientPresenceManager {
    void setPresent(UUID accountUuid, long deviceId, DisplacedPresenceListener displacementListener);

    boolean isPresent(UUID accountUuid, long deviceId);

    boolean clearPresence(UUID accountUuid, long deviceId);
}
