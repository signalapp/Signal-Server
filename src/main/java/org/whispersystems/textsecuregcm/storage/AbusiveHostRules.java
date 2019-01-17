package org.whispersystems.textsecuregcm.storage;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public abstract class AbusiveHostRules {

  private static final String ID      = "id";
  private static final String HOST    = "host";
  private static final String BLOCKED = "blocked";
  private static final String REGIONS = "regions";

  @Mapper(AbusiveHostRuleMapper.class)
  @SqlQuery("SELECT * FROM abusive_host_rules WHERE :host::inet <<= " + HOST)
  public abstract List<AbusiveHostRule> getAbusiveHostRulesFor(@Bind("host") String host);

  public static class AbusiveHostRuleMapper implements ResultSetMapper<AbusiveHostRule> {
    @Override
    public AbusiveHostRule map(int i, ResultSet resultSet, StatementContext statementContext)
        throws SQLException
    {
      String regionsData = resultSet.getString(REGIONS);

      List<String> regions;

      if (regionsData == null) regions = new LinkedList<>();
      else                     regions = Arrays.asList(regionsData.split(","));


      return new AbusiveHostRule(resultSet.getString(HOST), resultSet.getInt(BLOCKED) == 1, regions);
    }
  }

}
