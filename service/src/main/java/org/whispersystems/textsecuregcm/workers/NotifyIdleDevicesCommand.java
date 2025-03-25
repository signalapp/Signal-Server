package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.DynamoDbTables;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalTime;

public class NotifyIdleDevicesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private static final int DEFAULT_MAX_CONCURRENCY = 16;

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final LocalTime PREFERRED_NOTIFICATION_TIME = LocalTime.of(14, 0);

  private static final Counter DEVICE_INSPECTED_COUNTER =
      Metrics.counter(MetricsUtil.name(NotifyIdleDevicesCommand.class, "deviceInspected"));

  private static final String SCHEDULED_NOTIFICATION_COUNTER_NAME =
      MetricsUtil.name(NotifyIdleDevicesCommand.class, "scheduleNotification");

  private static final String DRY_RUN_TAG_NAME = "dryRun";

  private static final Logger log = LoggerFactory.getLogger(NotifyIdleDevicesCommand.class);

  public NotifyIdleDevicesCommand() {
    super("notify-idle-devices", "Schedules push notifications for idle devices");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for DynamoDB operations");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually schedule notifications");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);

    final MessagesManager messagesManager = getCommandDependencies().messagesManager();
    final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler = buildIdleDeviceNotificationScheduler();
    final Clock clock = getClock();
    final IdleWakeupEligibilityChecker idleWakeupEligibilityChecker = new IdleWakeupEligibilityChecker(clock, messagesManager);

    accounts
        .flatMap(account -> Flux.fromIterable(account.getDevices()).map(device -> Tuples.of(account, device)))
        .doOnNext(ignored -> DEVICE_INSPECTED_COUNTER.increment())
        .flatMap(accountAndDevice -> Mono.fromFuture(() ->
                    idleWakeupEligibilityChecker.isDeviceEligible(accountAndDevice.getT1(), accountAndDevice.getT2()))
                .mapNotNull(eligible -> eligible ? accountAndDevice : null)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                .onErrorResume(throwable -> {
                  log.warn("Failed to check eligibility for {}:{}",
                      accountAndDevice.getT1().getIdentifier(IdentityType.ACI),
                      accountAndDevice.getT2().getId(),
                      throwable);

                  return Mono.empty();
                }),
            maxConcurrency)
        .flatMap(accountAndDevice -> {
          final Account account = accountAndDevice.getT1();
          final Device device = accountAndDevice.getT2();

          final Mono<Void> scheduleNotificationMono = dryRun
              ? Mono.empty()
              : Mono.fromFuture(() -> idleDeviceNotificationScheduler.scheduleNotification(account, device, PREFERRED_NOTIFICATION_TIME))
                  .onErrorResume(throwable -> {
                    log.warn("Failed to schedule notification for {}:{}",
                        account.getIdentifier(IdentityType.ACI),
                        device.getId(),
                        throwable);

                    return Mono.empty();
                  });

          return scheduleNotificationMono
              .doOnSuccess(ignored -> Metrics.counter(SCHEDULED_NOTIFICATION_COUNTER_NAME,
                  DRY_RUN_TAG_NAME, String.valueOf(dryRun))
                  .increment());
        }, maxConcurrency)
        .then()
        .block();
  }

  @VisibleForTesting
  protected Clock getClock() {
    return Clock.systemUTC();
  }

  @VisibleForTesting
  protected IdleDeviceNotificationScheduler buildIdleDeviceNotificationScheduler() {
    final DynamoDbTables.TableWithExpiration tableConfiguration = getConfiguration().getDynamoDbTables().getScheduledJobs();

    return new IdleDeviceNotificationScheduler(
        getCommandDependencies().accountsManager(),
        getCommandDependencies().pushNotificationManager(),
        getCommandDependencies().dynamoDbAsyncClient(),
        tableConfiguration.getTableName(),
        tableConfiguration.getExpiration(),
        Clock.systemUTC());
  }

}
