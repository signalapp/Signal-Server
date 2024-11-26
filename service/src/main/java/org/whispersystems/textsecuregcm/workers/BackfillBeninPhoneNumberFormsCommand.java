package org.whispersystems.textsecuregcm.workers;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.PhoneNumberIdentifiers;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

public class BackfillBeninPhoneNumberFormsCommand extends AbstractCommandWithDependencies {
  private static final String BENIN_PREFIX = "+229";
  private static final PhoneNumberUtil PHONE_NUMBER_UTIL = PhoneNumberUtil.getInstance();

  private static final String SEGMENT_COUNT_ARGUMENT = "segments";
  private static final String PREFIX_ARGUMENT = "prefix";
  private static final String DRY_RUN_ARGUMENT = "dry-run";
  private static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  private static final String BUFFER_ARGUMENT = "buffer";

  private static final String PHONE_NUMBERS_INSPECTED = MetricsUtil.name(BackfillBeninPhoneNumberFormsCommand.class, "phoneNumbersInspected");
  private static final String PHONE_NUMBERS_BACKFILLED = MetricsUtil.name(BackfillBeninPhoneNumberFormsCommand.class, "phoneNumbersBackfilled");
  private static final String DRY_RUN_TAG = "dryRun";
  private static final String IS_OLD_FORMAT_TAG = "oldFormat";

  private static final Logger logger = LoggerFactory.getLogger(BackfillBeninPhoneNumberFormsCommand.class);

  public BackfillBeninPhoneNumberFormsCommand() {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration whisperServerConfiguration, final Environment environment)
          throws Exception {

      }
    }, "backfill-alternate-phone-number-forms", "Inserts alternate forms of existing phone numbers");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--segments")
        .type(Integer.class)
        .dest(SEGMENT_COUNT_ARGUMENT)
        .required(false)
        .setDefault(1)
        .help("The total number of segments for a DynamoDB scan");

    subparser.addArgument("--prefix")
        .type(String.class)
        .dest(PREFIX_ARGUMENT)
        .required(true)
        .help("The phone number prefix (including +) to filter by");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(16)
        .help("Max concurrency for backfilling PNI for alternate phone number forms");

    subparser.addArgument("--buffer")
        .type(Integer.class)
        .dest(BUFFER_ARGUMENT)
        .setDefault(16_384)
        .help("Records to buffer");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don’t actually insert any new PNI records");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {
    final int segments = namespace.getInt(SEGMENT_COUNT_ARGUMENT);
    final int concurrency = namespace.getInt(MAX_CONCURRENCY_ARGUMENT);
    final int bufferSize = namespace.getInt(BUFFER_ARGUMENT);
    final boolean dryRun = namespace.getBoolean(DRY_RUN_ARGUMENT);

    final Counter phoneNumbersInspectedCounter =
        Metrics.counter(PHONE_NUMBERS_INSPECTED, DRY_RUN_TAG, String.valueOf(dryRun));

    final Counter phoneNumbersBackfilledCounter =
        Metrics.counter(PHONE_NUMBERS_BACKFILLED, DRY_RUN_TAG, String.valueOf(dryRun));

    final PhoneNumberIdentifiers phoneNumberIdentifiers = commandDependencies.phoneNumberIdentifiers();

    phoneNumberIdentifiers.getPhoneNumberIdentifiers(BENIN_PREFIX, segments, Schedulers.parallel())
        .doOnNext(ignored -> phoneNumbersInspectedCounter.increment())
        .buffer(bufferSize)
        .map(source -> {
          final ArrayList<Tuple2<String, UUID>> shuffled = new ArrayList<>(source);
          Collections.shuffle(shuffled);
          return shuffled;
        })
        .limitRate(2)
        .flatMapIterable(Function.identity())
        .flatMap(tuple -> {
          final String e164 = tuple.getT1();
          final UUID pni = tuple.getT2();

          final boolean isNew = isNewFormatBeninNumber(e164);
          Metrics.counter(PHONE_NUMBERS_INSPECTED,
              DRY_RUN_TAG, String.valueOf(dryRun),
              IS_OLD_FORMAT_TAG, String.valueOf(!isNew));

          if (isNew) {
            // only old format numbers need to be backfilled
            return Mono.just(false);
          }

          return dryRun
              ? Mono.just(true)
              : Mono.fromFuture( () ->
                      phoneNumberIdentifiers.backfillAlternatePhoneNumbers(e164, pni).thenApply(ignored -> true))
                  .onErrorResume(t -> {
                    logger.warn("Failed to insert PNI for alternate forms of number {}", e164, t);
                    return Mono.just(false);
                  });
        }, concurrency)
        .filter(succeeded -> succeeded)
        .doOnNext(ignored -> phoneNumbersBackfilledCounter.increment())
        .then()
        .block();
  }

  private static boolean isNewFormatBeninNumber(final String number) {
    final Phonenumber.PhoneNumber phoneNumber;
    try {
      phoneNumber = PHONE_NUMBER_UTIL.parse(number, null);
      if ("BJ".equals(PHONE_NUMBER_UTIL.getRegionCodeForNumber(phoneNumber))) {
        final String nationalSignificantNumber = PHONE_NUMBER_UTIL.getNationalSignificantNumber(phoneNumber);
        return nationalSignificantNumber.length() == 10 && nationalSignificantNumber.startsWith("01");
      }
    } catch (NumberParseException e) {
      logger.error("Failed to parse benin phone number {}", number);
    }
    return false;
  }
}
