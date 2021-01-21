/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.whispersystems.textsecuregcm.entities.PreKey;

import java.util.List;

public interface PreKeyStore {

    void store(Account account, long deviceId, List<PreKey> keys);

    int getCount(Account account, long deviceId);

    List<KeyRecord> take(Account account, long deviceId);

    List<KeyRecord> take(Account account);

    void delete(Account account);
}
