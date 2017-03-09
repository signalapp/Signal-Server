/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.util;

import io.minio.MinioClient;
import org.whispersystems.textsecuregcm.configuration.S3Configuration;

import java.net.URL;

public class UrlSigner {

  private static final int   DURATION = 60 * 60;

  private final String accessKey;
  private final String accessSecret;
  private final String bucket;
  private final String providerUrl;

  public UrlSigner(S3Configuration config) {
    this.accessKey    = config.getAccessKey();
    this.accessSecret = config.getAccessSecret();
    this.bucket       = config.getAttachmentsBucket();
    this.providerUrl  = config.getProviderUrl();
  }

  public URL getPreSignedGetUrl(long attachmentId)
      throws Exception
  {
    MinioClient client = new MinioClient(providerUrl, accessKey, accessSecret);
    String request = client.presignedGetObject(bucket, String.valueOf(attachmentId), DURATION);
    return new URL(request);
  }

  public URL getPreSignedPutUrl(long attachmentId)
      throws Exception
  {
    MinioClient client = new MinioClient(providerUrl, accessKey, accessSecret);
    String request = client.presignedPutObject(bucket, String.valueOf(attachmentId), DURATION);
    return new URL(request);
  }
}
