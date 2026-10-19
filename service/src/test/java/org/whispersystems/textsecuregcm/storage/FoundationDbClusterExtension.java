/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Range;
import java.io.IOException;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.whispersystems.textsecuregcm.configuration.BulkheadConfiguration;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.storage.foundationdb.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FoundationDbClusterExtension implements AutoCloseable, BeforeAllCallback, BeforeEachCallback {

  private final FoundationDbDatabaseLifecycleManager[] databaseLifecycleManagers;
  private final FaultTolerantDatabase[] databases;

  private final CircuitBreakerConfiguration circuitBreakerConfiguration;
  private final BulkheadConfiguration bulkheadConfiguration;

  private static final Range ALL_KEYS_RANGE = new Range(new byte[] {}, new byte[] {(byte) 0xff});

  public FoundationDbClusterExtension() {
    this(1);
  }

  public FoundationDbClusterExtension(final int numInstances) {
    this(numInstances, new CircuitBreakerConfiguration(), new BulkheadConfiguration());
  }

  public FoundationDbClusterExtension(final int numInstances,
      final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final BulkheadConfiguration bulkheadConfiguration) {

    this.databaseLifecycleManagers = new FoundationDbDatabaseLifecycleManager[numInstances];
    this.databases = new FaultTolerantDatabase[numInstances];

    this.circuitBreakerConfiguration = circuitBreakerConfiguration;
    this.bulkheadConfiguration = bulkheadConfiguration;
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws IOException {
    final String resilienceConfigurationName = "foundationdb-test-" + RandomStringUtils.insecure().nextAlphanumeric(6);

    ResilienceUtil.getCircuitBreakerRegistry().addConfiguration(resilienceConfigurationName,
        circuitBreakerConfiguration.toCircuitBreakerConfig());

    ResilienceUtil.getBulkheadRegistry().addConfiguration(resilienceConfigurationName,
        bulkheadConfiguration.toBulkheadConfig().build());

    if (databaseLifecycleManagers[0] == null) {
      final String serviceContainerNamePrefix = System.getProperty("foundationDb.serviceContainerNamePrefix");

      for (int i = 0; i < databaseLifecycleManagers.length; i++) {
        final FoundationDbDatabaseLifecycleManager databaseLifecycleManager = serviceContainerNamePrefix != null
                ? new ServiceContainerFoundationDbDatabaseLifecycleManager(serviceContainerNamePrefix + i)
                : new TestcontainersFoundationDbDatabaseLifecycleManager();
        databaseLifecycleManager.initializeDatabase(FDB.selectAPIVersion(FoundationDbVersion.getFoundationDbApiVersion()));
        databaseLifecycleManagers[i] = databaseLifecycleManager;
        databases[i] = new FaultTolerantDatabase(databaseLifecycleManager.getDatabase(), String.format("messages-%d", i), resilienceConfigurationName, resilienceConfigurationName);
      }

    }
  }

  @Override
  public void beforeEach(final ExtensionContext context) throws Exception {
    for (final FaultTolerantDatabase database : databases) {
      database.run(transaction -> {
        transaction.clear(ALL_KEYS_RANGE);
        return null;
      }, FaultTolerantDatabase.Context.TEST);
    }
  }

  public FaultTolerantDatabase[] getDatabases() {
    return databases;
  }

  @Override
  public void close() {
    if (databaseLifecycleManagers[0] != null) {
      for (final FoundationDbDatabaseLifecycleManager databaseLifecycleManager : databaseLifecycleManagers) {
        databaseLifecycleManager.closeDatabase();
      }
    }
  }
}
