/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.dropwizard.health;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.health.HealthEnvironment;
import io.dropwizard.health.response.HealthResponderFactory;
import io.dropwizard.health.response.HealthResponseProvider;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import java.util.Collection;

@JsonTypeName("none")
public class NoneHealthResponder implements HealthResponderFactory {

  @Override
  public void configure(final String name, final Collection<String> healthCheckUrlPaths,
      final HealthResponseProvider healthResponseProvider, final HealthEnvironment health,
      final JerseyEnvironment jersey, final ServletEnvironment servlets, final ObjectMapper mapper) {
    // do nothing
  }
}
