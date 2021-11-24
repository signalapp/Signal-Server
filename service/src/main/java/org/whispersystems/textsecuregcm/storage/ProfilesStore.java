/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;
import java.util.UUID;

public interface ProfilesStore {

  void set(UUID uuid, VersionedProfile profile);

  Optional<VersionedProfile> get(UUID uuid, String version);

  void deleteAll(UUID uuid);
}
