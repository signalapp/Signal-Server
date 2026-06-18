/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = FoundationDbClusterConfiguration.class)
public interface FoundationDbDatabaseFactory extends Discoverable {

  Database build(final FDB fdb) throws IOException;
}
