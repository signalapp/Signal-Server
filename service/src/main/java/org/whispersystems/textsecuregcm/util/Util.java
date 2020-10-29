/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.util;

import com.google.i18n.phonenumbers.PhoneNumberUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

  private static final Pattern COUNTRY_CODE_PATTERN = Pattern.compile("^\\+([17]|2[07]|3[0123469]|4[013456789]|5[12345678]|6[0123456]|8[1246]|9[0123458]|\\d{3})");

  public static byte[] getContactToken(String number) {
    try {
      MessageDigest digest    = MessageDigest.getInstance("SHA1");
      byte[]        result    = digest.digest(number.getBytes());
      byte[]        truncated = Util.truncate(result, 10);

      return truncated;
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public static String getEncodedContactToken(String number) {
    return Base64.encodeBytesWithoutPadding(getContactToken(number));
  }

  public static boolean isValidNumber(String number) {
    return number.matches("^\\+[0-9]+") && PhoneNumberUtil.getInstance().isPossibleNumber(number, null);
  }

  public static String getCountryCode(String number) {
    Matcher matcher = COUNTRY_CODE_PATTERN.matcher(number);

    if (matcher.find()) return matcher.group(1);
    else                return "0";
  }

  public static String getNumberPrefix(String number) {
    String countryCode  = getCountryCode(number);
    int    remaining    = number.length() - (1 + countryCode.length());
    int    prefixLength = Math.min(4, remaining);

    return number.substring(0, 1 + countryCode.length() + prefixLength);
  }

  public static String encodeFormParams(Map<String, String> params) {
    try {
      StringBuffer buffer = new StringBuffer();

      for (String key : params.keySet()) {
        buffer.append(String.format("%s=%s",
                                    URLEncoder.encode(key, "UTF-8"),
                                    URLEncoder.encode(params.get(key), "UTF-8")));
        buffer.append("&");
      }

      buffer.deleteCharAt(buffer.length()-1);
      return buffer.toString();
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }

  public static boolean isEmpty(String param) {
    return param == null || param.length() == 0;
  }

  public static byte[] combine(byte[] one, byte[] two, byte[] three, byte[] four) {
    byte[] combined = new byte[one.length + two.length + three.length + four.length];
    System.arraycopy(one, 0, combined, 0, one.length);
    System.arraycopy(two, 0, combined, one.length, two.length);
    System.arraycopy(three, 0, combined, one.length + two.length, three.length);
    System.arraycopy(four, 0, combined, one.length + two.length + three.length, four.length);

    return combined;
  }

  public static byte[] truncate(byte[] element, int length) {
    byte[] result = new byte[length];
    System.arraycopy(element, 0, result, 0, result.length);

    return result;
  }


  public static byte[][] split(byte[] input, int firstLength, int secondLength) {
    byte[][] parts = new byte[2][];

    parts[0] = new byte[firstLength];
    System.arraycopy(input, 0, parts[0], 0, firstLength);

    parts[1] = new byte[secondLength];
    System.arraycopy(input, firstLength, parts[1], 0, secondLength);

    return parts;
  }

  public static byte[][] split(byte[] input, int firstLength, int secondLength, int thirdLength, int fourthLength) {
    byte[][] parts = new byte[4][];

    parts[0] = new byte[firstLength];
    System.arraycopy(input, 0, parts[0], 0, firstLength);

    parts[1] = new byte[secondLength];
    System.arraycopy(input, firstLength, parts[1], 0, secondLength);

    parts[2] = new byte[thirdLength];
    System.arraycopy(input, firstLength + secondLength, parts[2], 0, thirdLength);

    parts[3] = new byte[fourthLength];
    System.arraycopy(input, firstLength + secondLength + thirdLength, parts[3], 0, fourthLength);

    return parts;
  }

  public static byte[] generateSecretBytes(int size) {
    byte[] data = new byte[size];
    new SecureRandom().nextBytes(data);
    return data;
  }

  public static int toIntExact(long value) {
    if ((int)value != value) {
      throw new ArithmeticException("integer overflow");
    }
    return (int)value;
  }

  public static int currentDaysSinceEpoch() {
    return Util.toIntExact(System.currentTimeMillis() / 1000 / 60 / 60 / 24);
  }

  public static void sleep(long i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException ie) {}
  }

  public static void wait(Object object) {
    try {
      object.wait();
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public static void wait(Object object, long timeoutMs) {
    try {
      object.wait(timeoutMs);
    } catch (InterruptedException e) {
      throw new AssertionError(e);
    }
  }

  public static int hashCode(Object... objects) {
    return Arrays.hashCode(objects);
  }

  public static boolean isEquals(Object first, Object second) {
    return (first == null && second == null) || (first == second) || (first != null && first.equals(second));
  }

  public static long todayInMillis() {
    return todayInMillis(Clock.systemUTC());
  }

  public static long todayInMillis(Clock clock) {
    return TimeUnit.DAYS.toMillis(TimeUnit.MILLISECONDS.toDays(clock.instant().toEpochMilli()));
  }

  public static long todayInMillisGivenOffsetFromNow(Clock clock, Duration offset) {
    final long currentTimeSeconds = offset.addTo(clock.instant()).getLong(ChronoField.INSTANT_SECONDS);
    return TimeUnit.DAYS.toMillis(TimeUnit.SECONDS.toDays(currentTimeSeconds));
  }
}
