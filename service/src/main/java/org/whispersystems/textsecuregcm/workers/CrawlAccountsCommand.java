/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static com.codahale.metrics.MetricRegistry.name;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;

public class CrawlAccountsCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private static final String CRAWL_TYPE = "crawlType";
  private static final String WORKER_COUNT = "workers";

  private static final Logger logger = LoggerFactory.getLogger(CrawlAccountsCommand.class);

  public enum CrawlType implements ArgumentType<CrawlType> {
    GENERAL_PURPOSE,
    ACCOUNT_CLEANER,
    ;

    @Override
    public CrawlType convert(final ArgumentParser parser, final Argument arg, final String value)
        throws ArgumentParserException {
      return CrawlType.valueOf(value);
    }
  }

  public CrawlAccountsCommand() {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) throws Exception {

      }
    }, "crawl-accounts", "Runs account crawler tasks");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);
    subparser.addArgument("--crawl-type")
        .type(CrawlType.class)
        .dest(CRAWL_TYPE)
        .required(true)
        .help("The type of crawl to perform");

    subparser.addArgument("--workers")
        .type(Integer.class)
        .dest(WORKER_COUNT)
        .required(true)
        .help("The number of worker threads");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {

    UncaughtExceptionHandler.register();

    MetricsUtil.configureRegistries(configuration, environment);

    final CommandDependencies deps = CommandDependencies.build("account-crawler", environment, configuration);
    final AccountsManager accountsManager = deps.accountsManager();

    final FaultTolerantRedisCluster cacheCluster = deps.cacheCluster();
    final FaultTolerantRedisCluster metricsCluster = new FaultTolerantRedisCluster("metrics_cluster",
        configuration.getMetricsClusterConfiguration(), deps.redisClusterClientResources());

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(configuration.getAppConfig().getApplication(),
            configuration.getAppConfig().getEnvironment(),
            configuration.getAppConfig().getConfigurationName(),
            DynamicConfiguration.class);

    dynamicConfigurationManager.start();
    MetricsUtil.registerSystemResourceMetrics(environment);

    final int workers = Objects.requireNonNull(namespace.getInt(WORKER_COUNT));

    final AccountDatabaseCrawler crawler = switch ((CrawlType) namespace.get(CRAWL_TYPE)) {
      case GENERAL_PURPOSE -> {
        final ExecutorService pushFeedbackUpdateExecutor = environment.lifecycle()
            .executorService(name(getClass(), "pushFeedback-%d")).maxThreads(workers).minThreads(workers).build();

        // TODO listeners must be ordered so that ones that directly update accounts come last, so that read-only ones are not working with stale data
        final List<AccountDatabaseCrawlerListener> accountDatabaseCrawlerListeners = List.of(
            // PushFeedbackProcessor may update device properties
            new PushFeedbackProcessor(accountsManager, pushFeedbackUpdateExecutor));

        final AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(
            cacheCluster,
            AccountDatabaseCrawlerCache.GENERAL_PURPOSE_PREFIX);

        yield new AccountDatabaseCrawler("General-purpose account crawler",
            accountsManager,
            accountDatabaseCrawlerCache, accountDatabaseCrawlerListeners,
            configuration.getAccountDatabaseCrawlerConfiguration().getChunkSize()
        );
      }
      case ACCOUNT_CLEANER -> {
        final ExecutorService accountDeletionExecutor = environment.lifecycle()
            .executorService(name(getClass(), "accountCleaner-%d")).maxThreads(workers).minThreads(workers).build();

        final AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(
            cacheCluster, AccountDatabaseCrawlerCache.ACCOUNT_CLEANER_PREFIX);

        yield new AccountDatabaseCrawler("Account cleaner crawler",
            accountsManager,
            accountDatabaseCrawlerCache,
            List.of(new AccountCleaner(accountsManager, accountDeletionExecutor)),
            configuration.getAccountDatabaseCrawlerConfiguration().getChunkSize()
        );
      }
    };

    environment.lifecycle().manage(new CommandStopListener(configuration.getCommandStopListener()));

    environment.lifecycle().getManagedObjects().forEach(managedObject -> {
      try {
        managedObject.start();
      } catch (final Exception e) {
        logger.error("Failed to start managed object", e);
        throw new RuntimeException(e);
      }
    });

    try {
      crawler.crawlAllAccounts();
    } catch (final Exception e) {
      LoggerFactory.getLogger(CrawlAccountsCommand.class).error("Error crawling accounts", e);
    }

    environment.lifecycle().getManagedObjects().forEach(managedObject -> {
      try {
        managedObject.stop();
      } catch (final Exception e) {
        logger.error("Failed to stop managed object", e);
      }
    });
  }
}
