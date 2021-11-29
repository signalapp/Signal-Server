/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.List;

public interface RemoteConfigStore {

  void set(RemoteConfig remoteConfig);

  List<RemoteConfig> getAll();

  void delete(String name);
}
