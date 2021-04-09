/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.s3;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;

import java.net.URL;
import java.util.Date;

public class UrlSigner {

  private static final long   DURATION = 60 * 60 * 1000;

  private final AWSCredentials credentials;
  private final String bucket;

  public UrlSigner(String accessKey, String accessSecret, String bucket) {
    this.credentials = new BasicAWSCredentials(accessKey, accessSecret);
    this.bucket      = bucket;
  }

  public URL getPreSignedUrl(long attachmentId, HttpMethod method, boolean unaccelerated) {
    AmazonS3                    client  = new AmazonS3Client(credentials);
    GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucket, String.valueOf(attachmentId), method);
    
    request.setExpiration(new Date(System.currentTimeMillis() + DURATION));
    request.setContentType("application/octet-stream");

    if (unaccelerated) {
      client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
    } else {
      client.setS3ClientOptions(S3ClientOptions.builder().setAccelerateModeEnabled(true).build());
    }

    return client.generatePresignedUrl(request);
  }

}
