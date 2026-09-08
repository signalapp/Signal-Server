/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.List;
import java.util.Optional;

/// Configuration for FoundationDB external clients. Primarily exists because an external client must be specified to
/// make use of the multi-threaded client feature.
///
/// @param clientLibraryPaths a list of paths to external client libraries (generally .so files on Linux). This allows
/// us to run multiple FDB client versions concurrently if needed; however, during normal operation, we expect the list
/// to be a single path pointing to the current FDB client library
/// @param threadsPerClient   the number of networking threads spawned per client
public record FoundationDbExternalClientConfiguration(@NotEmpty List<@NotBlank String> clientLibraryPaths,
                                                      Optional<@PositiveOrZero Integer> threadsPerClient) {}
