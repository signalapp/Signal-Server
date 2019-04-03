package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.storage.mappers.AbusiveHostRuleRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class AbusiveHostRules {

  public static final String ID      = "id";
  public static final String HOST    = "host";
  public static final String BLOCKED = "blocked";
  public static final String REGIONS = "regions";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          getTimer       = metricRegistry.timer(name(AbusiveHostRules.class, "get"));

  private final Jdbi database;

  public AbusiveHostRules(Jdbi database) {
    this.database = database;
    this.database.registerRowMapper(new AbusiveHostRuleRowMapper());
  }

  public List<AbusiveHostRule> getAbusiveHostRulesFor(String host) {
    return database.withHandle(handle -> {
      try (Timer.Context timer = getTimer.time()) {
        return handle.createQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
                     .bind("host", host)
                     .mapTo(AbusiveHostRule.class)
                     .list();
      }
    });
  }

}
