package org.whispersystems.textsecuregcm.workers;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.Accounts;

import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jdbi.ImmutableListContainerFactory;
import io.dropwizard.jdbi.ImmutableSetContainerFactory;
import io.dropwizard.jdbi.OptionalContainerFactory;
import io.dropwizard.jdbi.args.OptionalArgumentFactory;
import io.dropwizard.metrics.ReporterFactory;
import io.dropwizard.setup.Environment;

public class PeriodicStatsCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(PeriodicStatsCommand.class);

  public PeriodicStatsCommand() {
    super(new Application<WhisperServerConfiguration>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment)
          throws Exception
      {

      }
    }, "stats", "Update periodic stats.");
  }

  @Override
  protected void run(Environment environment, Namespace namespace,
                     WhisperServerConfiguration configuration)
      throws Exception
  {
    try {
      environment.getObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      DataSourceFactory dbConfig = configuration.getReadDataSourceFactory();

      if (dbConfig == null) {
        logger.warn("No slave database configuration found!");
        return;
      }

      DBI dbi = new DBI(dbConfig.getUrl(), dbConfig.getUser(), dbConfig.getPassword());
      dbi.registerArgumentFactory(new OptionalArgumentFactory(dbConfig.getDriverClass()));
      dbi.registerContainerFactory(new ImmutableListContainerFactory());
      dbi.registerContainerFactory(new ImmutableSetContainerFactory());
      dbi.registerContainerFactory(new OptionalContainerFactory());

      Accounts accounts  = dbi.onDemand(Accounts.class);
      long     yesterday = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis()) - 1;
      long     monthAgo  = yesterday - 30;

      logger.info("Calculating daily active");
      final int dailyActiveAndroid = accounts.getAndroidActiveSinceCount(TimeUnit.DAYS.toMillis(yesterday));
      final int dailyActiveIos     = accounts.getIosActiveSinceCount(TimeUnit.DAYS.toMillis(yesterday));

      logger.info("Calculating monthly active");
      final int monthlyActiveAndroid = accounts.getAndroidActiveSinceCount(TimeUnit.DAYS.toMillis(monthAgo));
      final int monthlyActiveIos     = accounts.getIosActiveSinceCount(TimeUnit.DAYS.toMillis(monthAgo));

      logger.info("Calculating daily signed keys");
      final int dailyActiveNoSignedKeys   = accounts.getUnsignedKeysCount(TimeUnit.DAYS.toMillis(yesterday));
      logger.info("Calculating monthly signed keys");
      final int monthlyActiveNoSignedKeys = accounts.getUnsignedKeysCount(TimeUnit.DAYS.toMillis(monthAgo ));

      environment.metrics().register(name(PeriodicStatsCommand.class, "daily_active"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return dailyActiveAndroid + dailyActiveIos;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "daily_active_android"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return dailyActiveAndroid;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "daily_active_ios"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return dailyActiveIos;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "monthly_active"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return monthlyActiveAndroid + monthlyActiveIos;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "monthly_active_android"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return monthlyActiveAndroid;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "monthly_active_ios"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return monthlyActiveIos;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "daily_no_signed_keys"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return dailyActiveNoSignedKeys;
                                       }
                                     });

      environment.metrics().register(name(PeriodicStatsCommand.class, "monthly_no_signed_keys"),
                                     new Gauge<Integer>() {
                                       @Override
                                       public Integer getValue() {
                                         return monthlyActiveNoSignedKeys;
                                       }
                                     });


      for (ReporterFactory reporterFactory : configuration.getMetricsFactory().getReporters()) {
        ScheduledReporter reporter = reporterFactory.build(environment.metrics());
        logger.info("Reporting via: " + reporter);
        reporter.report();
        logger.info("Reporting finished...");
      }

    } catch (Exception ex) {
      logger.warn("Directory Exception", ex);
      throw new RuntimeException(ex);
    } finally {
      Thread.sleep(3000);
      System.exit(0);
    }
  }
}
