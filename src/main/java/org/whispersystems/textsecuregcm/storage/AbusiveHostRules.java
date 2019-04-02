package org.whispersystems.textsecuregcm.storage;

import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.storage.mappers.AbusiveHostRuleRowMapper;

import java.util.List;

public class AbusiveHostRules {

  public static final String ID      = "id";
  public static final String HOST    = "host";
  public static final String BLOCKED = "blocked";
  public static final String REGIONS = "regions";

  private final Jdbi database;

  public AbusiveHostRules(Jdbi database) {
    this.database = database;
    this.database.registerRowMapper(new AbusiveHostRuleRowMapper());
  }

  public List<AbusiveHostRule> getAbusiveHostRulesFor(String host) {
    return database.withHandle(handle -> handle.createQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
                                               .bind("host", host)
                                               .mapTo(AbusiveHostRule.class)
                                               .list());
  }

}
