/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import java.util.Objects;
import java.util.Optional;

public class AssessmentResult {

  private final boolean solved;
  private final float actualScore;
  private final float defaultScoreThreshold;
  private final String scoreString;

  /**
   * A captcha assessment
   *
   * @param solved if false, the captcha was not successfully completed
   * @param actualScore float representation of the risk level from [0, 1.0], with 1.0 being the least risky
   * @param defaultScoreThreshold  the score threshold which the score will be evaluated against by default
   * @param scoreString a quantized string representation of the risk level, suitable for use in metrics
   */
  private AssessmentResult(boolean solved, float actualScore, float defaultScoreThreshold, final String scoreString) {
    this.solved = solved;
    this.actualScore = actualScore;
    this.defaultScoreThreshold = defaultScoreThreshold;
    this.scoreString = scoreString;
  }

  /**
   * Construct an {@link AssessmentResult} from a captcha evaluation score
   *
   * @param actualScore the score
   * @param defaultScoreThreshold the threshold to compare the score against by default
   */
  public static AssessmentResult fromScore(float actualScore, float defaultScoreThreshold) {
    if (actualScore < 0 || actualScore > 1.0 || defaultScoreThreshold < 0 || defaultScoreThreshold > 1.0) {
      throw new IllegalArgumentException("invalid captcha score");
    }
    return new AssessmentResult(true, actualScore, defaultScoreThreshold, AssessmentResult.scoreString(actualScore));
  }

  /**
   * Construct a captcha assessment that will always be invalid
   */
  public static AssessmentResult invalid() {
    return new AssessmentResult(false, 0.0f, 0.0f, "");
  }

  /**
   * Construct a captcha assessment that will always be valid
   */
  public static AssessmentResult alwaysValid() {
    return new AssessmentResult(true, 1.0f, 0.0f, "1.0");
  }

  /**
   * Check if the captcha assessment should be accepted using the default score threshold
   *
   * @return true if this assessment should be accepted under the default score threshold
   */
  public boolean isValid() {
    return isValid(Optional.empty());
  }

  /**
   * Check if the captcha assessment should be accepted
   *
   * @param scoreThreshold the minimum score the assessment requires to pass, uses default if empty
   * @return true if the assessment scored higher than the provided scoreThreshold
   */
  public boolean isValid(Optional<Float> scoreThreshold) {
    if (!solved) {
      return false;
    }
    // Since HCaptcha scores are truncated to 2 decimal places, we can multiply by 100 and round to the nearest int for
    // comparison instead of floating point comparisons
    return normalizedIntScore(this.actualScore) >= normalizedIntScore(scoreThreshold.orElse(this.defaultScoreThreshold));
  }

  private static int normalizedIntScore(final float score) {
    return Math.round(score * 100);
  }

  public String getScoreString() {
    return scoreString;
  }

  public float getScore() {
    return this.actualScore;
  }


  /**
   * Map a captcha score in [0.0, 1.0] to a low cardinality discrete space in [0, 100] suitable for use in metrics
   */
  private static String scoreString(final float score) {
    final int x = Math.round(score * 10); // [0, 10]
    return Integer.toString(x * 10); // [0, 100] in increments of 10
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    AssessmentResult that = (AssessmentResult) o;
    return solved == that.solved && Float.compare(that.actualScore, actualScore) == 0
        && Float.compare(that.defaultScoreThreshold, defaultScoreThreshold) == 0 && Objects.equals(scoreString,
        that.scoreString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(solved, actualScore, defaultScoreThreshold, scoreString);
  }
}
