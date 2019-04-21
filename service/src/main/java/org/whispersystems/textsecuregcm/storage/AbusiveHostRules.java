package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.whispersystems.textsecuregcm.storage.mappers.AbusiveHostRuleRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class AbusiveHostRules {

  public static final String ID      = "id";
  public static final String HOST    = "host";
  public static final String BLOCKED = "blocked";
  public static final String REGIONS = "regions";
  public static final String NOTES   = "notes";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          getTimer       = metricRegistry.timer(name(AbusiveHostRules.class, "get"));
  private final Timer          insertTimer    = metricRegistry.timer(name(AbusiveHostRules.class, "setBlockedHost"));

  private final FaultTolerantDatabase database;

  public AbusiveHostRules(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new AbusiveHostRuleRowMapper());
  }

  public List<AbusiveHostRule> getAbusiveHostRulesFor(String host) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context timer = getTimer.time()) {
        return handle.createQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
                     .bind("host", host)
                     .mapTo(AbusiveHostRule.class)
                     .list();
      }
    }));
  }

  public void setBlockedHost(String host, String notes) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context timer = insertTimer.time()) {
        handle.createUpdate("INSERT INTO abusive_host_rules(host, blocked, notes) VALUES(:host::inet, :blocked, :notes) ON CONFLICT DO NOTHING")
              .bind("host", host)
              .bind("blocked", 1)
              .bind("notes", notes)
              .execute();
      }
    }));
  }

}
