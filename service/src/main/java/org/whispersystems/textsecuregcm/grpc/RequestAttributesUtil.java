package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class RequestAttributesUtil {

  static final Context.Key<RequestAttributes> REQUEST_ATTRIBUTES_CONTEXT_KEY = Context.key("request-attributes");

  private static final List<Locale> AVAILABLE_LOCALES = Arrays.asList(Locale.getAvailableLocales());

  /**
   * Returns the acceptable languages listed by the remote client in the current gRPC request context.
   *
   * @return the acceptable languages listed by the remote client; may be empty if unparseable or not specified
   */
  public static List<Locale.LanguageRange> getAcceptableLanguages() {
    return REQUEST_ATTRIBUTES_CONTEXT_KEY.get().acceptLanguage();
  }

  /**
   * Returns a list of distinct locales supported by the JVM and accepted by the remote client in the current gRPC
   * context. May be empty if the client did not supply a list of acceptable languages, if the list of acceptable
   * languages could not be parsed, or if none of the acceptable languages are available in the current JVM.
   *
   * @return a list of distinct locales acceptable to the remote client and available in this JVM
   */
  public static List<Locale> getAvailableAcceptedLocales() {
    return Locale.filter(getAcceptableLanguages(), AVAILABLE_LOCALES);
  }

  /**
   * Returns the remote address of the remote client in the current gRPC request context.
   *
   * @return the remote address of the remote client
   */
  public static InetAddress getRemoteAddress() {
    return REQUEST_ATTRIBUTES_CONTEXT_KEY.get().remoteAddress();
  }

  /**
   * Returns the unparsed user-agent of the remote client in the current gRPC request context.
   *
   * @return the unparsed user-agent of the remote client; may be empty if not specified
   */
  public static Optional<String> getUserAgent() {
    return Optional.ofNullable(REQUEST_ATTRIBUTES_CONTEXT_KEY.get().userAgent());
  }
}
