package org.whispersystems.textsecuregcm.grpc;

import com.google.common.net.HttpHeaders;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The request attributes interceptor makes common request attributes from call metadata available to service
 * implementations by attaching them to a {@link Context} attribute that can be read via {@link RequestAttributesUtil}.
 *
 * @see RequestAttributesUtil
 */
public class RequestAttributesInterceptor implements ServerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(RequestAttributesInterceptor.class);

  private static final Metadata.Key<String> ACCEPT_LANG_KEY =
      Metadata.Key.of(HttpHeaders.ACCEPT_LANGUAGE, Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> USER_AGENT_KEY =
      Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);

  private static final Metadata.Key<String> X_FORWARDED_FOR_KEY =
      Metadata.Key.of(HttpHeaders.X_FORWARDED_FOR, Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final String userAgentHeader = headers.get(USER_AGENT_KEY);
    final String acceptLanguageHeader = headers.get(ACCEPT_LANG_KEY);
    final String xForwardedForHeader = headers.get(X_FORWARDED_FOR_KEY);

    final Optional<InetAddress> remoteAddress = getMostRecentProxy(xForwardedForHeader)
        .flatMap(mostRecentProxy -> {
          try {
            return Optional.of(InetAddress.ofLiteral(mostRecentProxy));
          } catch (IllegalArgumentException e) {
            log.warn("Failed to parse most recent proxy {} as an IP address", mostRecentProxy, e);
            return Optional.empty();
          }
        })
        .or(() -> {
          log.warn("No usable X-Forwarded-For header present, using remote socket address");
          final SocketAddress socketAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
          if (socketAddress == null || !(socketAddress instanceof InetSocketAddress inetAddress)) {
            log.warn("Remote socket address not present or is not an inet address: {}", socketAddress);
            return Optional.empty();
          }
          return Optional.of(inetAddress.getAddress());
        });
    if (!remoteAddress.isPresent()) {
      return ServerInterceptorUtil.closeWithStatus(call, Status.UNAVAILABLE);
    }

    @Nullable List<Locale.LanguageRange> acceptLanguages = Collections.emptyList();
    if (StringUtils.isNotBlank(acceptLanguageHeader)) {
      try {
        acceptLanguages = Locale.LanguageRange.parse(acceptLanguageHeader);
      } catch (final IllegalArgumentException e) {
        log.debug("Invalid Accept-Language header from User-Agent {}: {}", userAgentHeader, acceptLanguageHeader, e);
      }
    }

    final RequestAttributes requestAttributes =
        new RequestAttributes(remoteAddress.get(), userAgentHeader, acceptLanguages);
    return Contexts.interceptCall(
        Context.current().withValue(RequestAttributesUtil.REQUEST_ATTRIBUTES_CONTEXT_KEY, requestAttributes),
        call, headers, next);
  }

  /**
     * Returns the most recent proxy in a chain described by an {@code X-Forwarded-For} header.
     *
     * @param forwardedFor the value of an X-Forwarded-For header
     * @return the IP address of the most recent proxy in the forwarding chain, or empty if none was found or
     * {@code forwardedFor} was null
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For">X-Forwarded-For - HTTP |
     * MDN</a>
     */
  public static Optional<String> getMostRecentProxy(@Nullable final String forwardedFor) {
    return Optional.ofNullable(forwardedFor)
        .map(ff -> {
          final int idx = forwardedFor.lastIndexOf(',') + 1;
          return idx < forwardedFor.length()
              ? forwardedFor.substring(idx).trim()
              : null;
        })
        .filter(StringUtils::isNotBlank);
      }
}
