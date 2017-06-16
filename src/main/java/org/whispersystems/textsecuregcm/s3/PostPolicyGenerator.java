package org.whispersystems.textsecuregcm.s3;

import org.apache.commons.codec.binary.Base64;
import org.whispersystems.textsecuregcm.util.Pair;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class PostPolicyGenerator {

  public  static final DateTimeFormatter AWS_DATE_TIME   = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX");
  private static final DateTimeFormatter CREDENTIAL_DATE = DateTimeFormatter.ofPattern("yyyyMMdd"          );

  private final String region;
  private final String bucket;
  private final String awsAccessId;

  public PostPolicyGenerator(String region, String bucket, String awsAccessId) {
    this.region      = region;
    this.bucket      = bucket;
    this.awsAccessId = awsAccessId;
  }

  public Pair<String, String> createFor(ZonedDateTime now, String object) {
    try {
      String expiration     = now.plusMinutes(30).format(DateTimeFormatter.ISO_INSTANT);
      String credentialDate = now.format(CREDENTIAL_DATE);
      String requestDate    = now.format(AWS_DATE_TIME  );
      String credential     = String.format("%s/%s/%s/s3/aws4_request", awsAccessId, credentialDate, region);

      String policy = String.format("{ \"expiration\": \"%s\",\n" +
                                        "  \"conditions\": [\n" +
                                        "    {\"bucket\": \"%s\"},\n" +
                                        "    {\"key\": \"%s\"},\n" +
                                        "    {\"acl\": \"private\"},\n" +
                                        "    [\"starts-with\", \"$Content-Type\", \"\"],\n" +
                                        "\n" +
                                        "    {\"x-amz-credential\": \"%s\"},\n" +
                                        "    {\"x-amz-algorithm\": \"AWS4-HMAC-SHA256\"},\n" +
                                        "    {\"x-amz-date\": \"%s\" }\n" +
                                        "  ]\n" +
                                        "}", expiration, bucket, object, credential, requestDate);

      return new Pair<>(credential, Base64.encodeBase64String(policy.getBytes("UTF-8")));
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }

}
