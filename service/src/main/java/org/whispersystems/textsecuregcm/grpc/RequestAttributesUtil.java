package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;

public class RequestAttributesUtil {

  static final Context.Key<List<Locale.LanguageRange>> ACCEPT_LANGUAGE_CONTEXT_KEY = Context.key("accept-language");
  static final Context.Key<InetAddress> REMOTE_ADDRESS_CONTEXT_KEY = Context.key("remote-address");
  static final Context.Key<UserAgent> USER_AGENT_CONTEXT_KEY = Context.key("user-agent");

  private static final List<Locale> AVAILABLE_LOCALES = Arrays.asList(Locale.getAvailableLocales());

  /**
   * Returns the acceptable languages listed by the remote client in the current gRPC request context.
   *
   * @return the acceptable languages listed by the remote client; may be empty if unparseable or not specified
   */
  public static Optional<List<Locale.LanguageRange>> getAcceptableLanguages() {
    return Optional.ofNullable(ACCEPT_LANGUAGE_CONTEXT_KEY.get());
  }

  /**
   * Returns a list of distinct locales supported by the JVM and accepted by the remote client in the current gRPC
   * context. May be empty if the client did not supply a list of acceptable languages, if the list of acceptable
   * languages could not be parsed, or if none of the acceptable languages are available in the current JVM.
   *
   * @return a list of distinct locales acceptable to the remote client and available in this JVM
   */
  public static List<Locale> getAvailableAcceptedLocales() {
    return getAcceptableLanguages()
        .map(languageRanges -> Locale.filter(languageRanges, AVAILABLE_LOCALES))
        .orElseGet(Collections::emptyList);
  }

  /**
   * Returns the remote address of the remote client in the current gRPC request context.
   *
   * @return the remote address of the remote client
   */
  public static InetAddress getRemoteAddress() {
    return REMOTE_ADDRESS_CONTEXT_KEY.get();
  }

  /**
   * Returns the parsed user-agent of the remote client in the current gRPC request context.
   *
   * @return the parsed user-agent of the remote client; may be null if unparseable or not specified
   */
  public static Optional<UserAgent> getUserAgent() {
    return Optional.ofNullable(USER_AGENT_CONTEXT_KEY.get());
  }
}
