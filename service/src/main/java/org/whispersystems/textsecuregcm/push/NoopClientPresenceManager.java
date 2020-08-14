package org.whispersystems.textsecuregcm.push;

import java.util.UUID;

public class NoopClientPresenceManager implements ClientPresenceManager {

    @Override
    public void setPresent(final UUID accountUuid, final long deviceId, final DisplacedPresenceListener displacementListener) {
    }

    @Override
    public boolean isPresent(final UUID accountUuid, final long deviceId) {
        return false;
    }

    @Override
    public boolean clearPresence(final UUID accountUuid, final long deviceId) {
        return false;
    }
}
