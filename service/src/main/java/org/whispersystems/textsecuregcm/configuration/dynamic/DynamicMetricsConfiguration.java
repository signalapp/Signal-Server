/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * @param enableLettuceRemoteTag whether the `remote` tag should be added. Note: although this is dynamic, meters are
 *                               cached after creation, so changes will only affect servers launched after the change.
 * @param enableAwsSdkMetrics whether to record AWS SDK metrics. Note: although this is dynamic, meters are cached after
 *                            creation, so changes will only affect servers launched after the change.
 * @param datadogAllowList if present, a list of metrics to send to datadog; others will be filtered out. If null, this filtering is not performed.
 */
public record DynamicMetricsConfiguration(boolean enableLettuceRemoteTag, boolean enableAwsSdkMetrics, @Nullable Set<String> datadogAllowList) {
}
