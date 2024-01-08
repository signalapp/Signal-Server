/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

public class HttpUtilsTest {

  @Test
  public void queryParameterStringPreservesOrder() {
    final String result = HttpUtils.queryParamString(List.of(
        Map.entry("a", "aval"),
        Map.entry("b", "bval1"),
        Map.entry("b", "bval2")
    ));
    // https://url.spec.whatwg.org/#example-constructing-urlsearchparams allows multiple parameters with the same key
    // https://url.spec.whatwg.org/#example-searchparams-sort implies that the relative order of values for parameters
    // with the same key must be preserved
    assertThat(result).isEqualTo("?a=aval&b=bval1&b=bval2");
  }

  @Test
  public void queryParameterStringEncodesUnsafeChars() {
    final String result = HttpUtils.queryParamString(List.of(Map.entry("&k?e=y/!", "=v/a?l&u;e")));
    assertThat(result).isEqualTo("?%26k%3Fe%3Dy%2F%21=%3Dv%2Fa%3Fl%26u%3Be");
  }
}
