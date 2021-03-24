package org.whispersystems.textsecuregcm.sms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.VoiceVerificationConfiguration;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
class TwilioVerifyExperimentEnrollmentManagerTest {

  private final ExperimentEnrollmentManager experimentEnrollmentManager = mock(ExperimentEnrollmentManager.class);
  private final VoiceVerificationConfiguration voiceVerificationConfiguration = mock(VoiceVerificationConfiguration.class);
  private TwilioVerifyExperimentEnrollmentManager manager;

  private static final String NUMBER = "+15055551212";

  private static final Optional<String> INELIGIBLE_CLIENT = Optional.of("android-2020-01");
  private static final Optional<String> ELIGIBLE_CLIENT = Optional.of("anything");

  private static final List<LanguageRange> LANGUAGE_ONLY_SUPPORTED_BY_SIGNAL = LanguageRange.parse("am");
  private static final List<LanguageRange> LANGUAGE_NOT_SUPPORTED_BY_SIGNAL_OR_TWILIO = LanguageRange.parse("xx");
  private static final List<LanguageRange> LANGUAGE_SUPPORTED_BY_TWILIO = LanguageRange.parse("fr-CA");


  @BeforeEach
  void setup() {
    when(voiceVerificationConfiguration.getLocales())
        .thenReturn(Set.of("am", "en-US", "fr-CA"));

    manager = new TwilioVerifyExperimentEnrollmentManager(
        voiceVerificationConfiguration,
        experimentEnrollmentManager);
  }

  @ParameterizedTest
  @MethodSource
  void testIsEnrolled(String message, boolean expected, Optional<String> clientType, String number,
      List<LanguageRange> languageRanges, String transport, boolean managerResponse) {

    when(experimentEnrollmentManager.isEnrolled(number, TwilioVerifyExperimentEnrollmentManager.EXPERIMENT_NAME))
        .thenReturn(managerResponse);
    assertEquals(expected, manager.isEnrolled(clientType, number, languageRanges, transport), message);
  }

  static Stream<Arguments> testIsEnrolled() {
    return Stream.of(
        Arguments.of("ineligible client", false, INELIGIBLE_CLIENT, NUMBER, Collections.emptyList(), "sms", true),
        Arguments
            .of("ineligible client", false, Optional.of("android-ng"), NUMBER, Collections.emptyList(), "sms", true),
        Arguments
            .of("client, language, and manager all agree on enrollment", true, ELIGIBLE_CLIENT, NUMBER,
                LANGUAGE_SUPPORTED_BY_TWILIO,
                "sms", true),

        Arguments
            .of("enrolled: ineligible language doesn’t matter with sms", true, ELIGIBLE_CLIENT, NUMBER,
                LANGUAGE_ONLY_SUPPORTED_BY_SIGNAL, "sms",
                true),

        Arguments
            .of("not enrolled: language only supported by Signal is preferred", false, ELIGIBLE_CLIENT, NUMBER, List.of(
                LANGUAGE_ONLY_SUPPORTED_BY_SIGNAL.get(0), LANGUAGE_SUPPORTED_BY_TWILIO.get(0)), "voice", true),

        Arguments.of("enrolled: preferred language is supported", true, ELIGIBLE_CLIENT, NUMBER, List.of(
            LANGUAGE_SUPPORTED_BY_TWILIO.get(0), LANGUAGE_ONLY_SUPPORTED_BY_SIGNAL
                .get(0)), "voice", true),

        Arguments
            .of("enrolled: preferred (and only) language is not supported by Signal or Twilio", true, ELIGIBLE_CLIENT,
                NUMBER, LANGUAGE_NOT_SUPPORTED_BY_SIGNAL_OR_TWILIO, "voice", true),

        Arguments.of("not enrolled: preferred language (and only) is only supported by Siganl", false, ELIGIBLE_CLIENT,
            NUMBER, LANGUAGE_ONLY_SUPPORTED_BY_SIGNAL, "voice", true)

    );
  }
}
