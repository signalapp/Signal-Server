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
import java.util.concurrent.ExecutorService;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.AccountCleaner;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawler;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerCache;
import org.whispersystems.textsecuregcm.storage.AccountDatabaseCrawlerListener;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.NonNormalizedAccountCrawlerListener;
import org.whispersystems.textsecuregcm.storage.PushFeedbackProcessor;

public class CrawlAccountsCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private static final String CRAWL_TYPE = "crawlType";

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
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {

    final CommandDependencies deps = CommandDependencies.build("account-crawler", environment, configuration);
    final AccountsManager accountsManager = deps.accountsManager();

    final FaultTolerantRedisCluster cacheCluster = deps.cacheCluster();
    final FaultTolerantRedisCluster metricsCluster = new FaultTolerantRedisCluster("metrics_cluster",
        configuration.getMetricsClusterConfiguration(), deps.redisClusterClientResources());

    // TODO listeners must be ordered so that ones that directly update accounts come last, so that read-only ones are not working with stale data
    final List<AccountDatabaseCrawlerListener> accountDatabaseCrawlerListeners = List.of(
        new NonNormalizedAccountCrawlerListener(accountsManager, metricsCluster),
        // PushFeedbackProcessor may update device properties
        new PushFeedbackProcessor(accountsManager));

    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(configuration.getAppConfig().getApplication(),
            configuration.getAppConfig().getEnvironment(),
            configuration.getAppConfig().getConfigurationName(),
            DynamicConfiguration.class);

    final AccountDatabaseCrawler crawler =

        switch ((CrawlType) namespace.get(CRAWL_TYPE)) {
          case GENERAL_PURPOSE -> {
            final AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(
                cacheCluster,
                AccountDatabaseCrawlerCache.GENERAL_PURPOSE_PREFIX);

            yield new AccountDatabaseCrawler("General-purpose account crawler",
                accountsManager,
                accountDatabaseCrawlerCache, accountDatabaseCrawlerListeners,
                configuration.getAccountDatabaseCrawlerConfiguration().getChunkSize(),
                dynamicConfigurationManager
            );
          }
          case ACCOUNT_CLEANER -> {
            final ExecutorService accountDeletionExecutor = environment.lifecycle()
                .executorService(name(getClass(), "accountCleaner-%d")).maxThreads(16).minThreads(16).build();

            final AccountDatabaseCrawlerCache accountDatabaseCrawlerCache = new AccountDatabaseCrawlerCache(
                cacheCluster, AccountDatabaseCrawlerCache.ACCOUNT_CLEANER_PREFIX);

            yield new AccountDatabaseCrawler("Account cleaner crawler",
                accountsManager,
                accountDatabaseCrawlerCache,
                List.of(new AccountCleaner(accountsManager, accountDeletionExecutor)),
                configuration.getAccountDatabaseCrawlerConfiguration().getChunkSize(),
                dynamicConfigurationManager
            );
          }
        };

    try {
      crawler.crawlAllAccounts();
    } catch (final Exception e) {
      LoggerFactory.getLogger(CrawlAccountsCommand.class).error("Error crawling accounts", e);
    }

  }
}
