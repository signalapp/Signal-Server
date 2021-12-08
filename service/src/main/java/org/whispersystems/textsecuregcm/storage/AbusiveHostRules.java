/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Suppliers;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.experiment.Experiment;
import org.whispersystems.textsecuregcm.storage.mappers.AbusiveHostRuleRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;

public class AbusiveHostRules {

  private static final Logger logger = LoggerFactory.getLogger(AbusiveHostRules.class);

  public static final String ID = "id";
  public static final String HOST = "host";
  public static final String BLOCKED = "blocked";
  public static final String REGIONS = "regions";
  public static final String NOTES = "notes";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer getTimer = metricRegistry.timer(name(AbusiveHostRules.class, "get"));
  private final Timer insertTimer = metricRegistry.timer(name(AbusiveHostRules.class, "setBlockedHost"));

  private final FaultTolerantDatabase oldDatabase;
  private final FaultTolerantDatabase newDatabase;

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final Experiment migrationExperiment = new Experiment("abusiveHostRulesMigration");

  public AbusiveHostRules(FaultTolerantDatabase oldDatabase, FaultTolerantDatabase newDatabase,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this.oldDatabase = oldDatabase;
    this.oldDatabase.getDatabase().registerRowMapper(new AbusiveHostRuleRowMapper());

    this.newDatabase = newDatabase;
    this.newDatabase.getDatabase().registerRowMapper(new AbusiveHostRuleRowMapper());

    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public List<AbusiveHostRule> getAbusiveHostRulesFor(String host) {
    final List<AbusiveHostRule> oldDbRules = oldDatabase.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context timer = getTimer.time()) {
        return handle.createQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
            .bind("host", host)
            .mapTo(AbusiveHostRule.class)
            .list();
      }
    }));

    final Supplier<List<AbusiveHostRule>> newDbRules = Suppliers.memoize(
        () -> newDatabase.with(jdbi -> jdbi.withHandle(
            handle -> handle.createQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
                .bind("host", host)
                .mapTo(AbusiveHostRule.class)
                .list())));

    if (dynamicConfigurationManager.getConfiguration().getAbusiveHostRulesMigrationConfiguration().isNewReadEnabled()) {
      migrationExperiment.compareSupplierResult(oldDbRules, newDbRules);
    }

    return dynamicConfigurationManager.getConfiguration().getAbusiveHostRulesMigrationConfiguration().isNewPrimary()
        ? newDbRules.get()
        : oldDbRules;
  }

  public void setBlockedHost(String host, String notes) {
    oldDatabase.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context timer = insertTimer.time()) {
        handle.createUpdate(
                "INSERT INTO abusive_host_rules(host, blocked, notes) VALUES(:host::inet, :blocked, :notes) ON CONFLICT DO NOTHING")
            .bind("host", host)
            .bind("blocked", 1)
            .bind("notes", notes)
            .execute();
      }
    }));

    if (dynamicConfigurationManager.getConfiguration().getAbusiveHostRulesMigrationConfiguration()
        .isNewWriteEnabled()) {
      try {
        newDatabase.use(jdbi -> jdbi.useHandle(handle -> handle.createUpdate(
                "INSERT INTO abusive_host_rules(host, blocked, notes) VALUES(:host::inet, :blocked, :notes) ON CONFLICT DO NOTHING")
            .bind("host", host)
            .bind("blocked", 1)
            .bind("notes", notes)
            .execute()));
      } catch (final Exception e) {
        logger.warn("Failed to insert rule in new database", e);
      }
    }
  }

  public int migrateAbusiveHostRule(AbusiveHostRule rule, String notes) {
    return newDatabase.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context timer = insertTimer.time()) {
        return handle.createUpdate(
                "INSERT INTO abusive_host_rules(host, blocked, notes, regions) VALUES(:host::inet, :blocked, :notes, :regions) ON CONFLICT DO NOTHING")
            .bind("host", rule.host())
            .bind("blocked", rule.blocked() ? 1 : 0)
            .bind("notes", notes)
            .bind("regions", String.join(",", rule.regions()))
            .execute();
      }
    }));
  }

  public void forEachInOldDatabase(final BiConsumer<AbusiveHostRule, String> consumer, final int fetchSize) {
    final AbusiveHostRuleRowMapper rowMapper = new AbusiveHostRuleRowMapper();

    oldDatabase.use(jdbi -> jdbi.useHandle(handle -> handle.useTransaction(transactionHandle ->
        transactionHandle.createQuery("SELECT * FROM abusive_host_rules")
            .setFetchSize(fetchSize)
            .map((resultSet, ctx) -> {
              AbusiveHostRule rule = rowMapper.map(resultSet, ctx);
              String notes = resultSet.getString(NOTES);
              return new Pair<>(rule, notes);
            })
            .forEach(pair -> consumer.accept(pair.first(), pair.second())))));
  }
}
