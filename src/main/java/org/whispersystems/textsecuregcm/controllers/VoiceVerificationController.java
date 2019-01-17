package org.whispersystems.textsecuregcm.controllers;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Path("/v1/voice/")
public class VoiceVerificationController {

  private static final String PLAY_TWIML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<Response>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Pause length=\"1\"/>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Pause length=\"1\"/>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "    <Play>%s</Play>\n" +
      "</Response>";


  private final String      baseUrl;
  private final Set<String> supportedLocales;

  public VoiceVerificationController(String baseUrl, Set<String> supportedLocales) {
    this.baseUrl          = baseUrl;
    this.supportedLocales = supportedLocales;
  }
  
  @POST
  @Path("/description/{code}")
  @Produces(MediaType.APPLICATION_XML)
  public Response getDescription(@PathParam("code") String code, @QueryParam("l") String locale) {
    code = code.replaceAll("[^0-9]", "");

    if (code.length() != 6) {
      return Response.status(400).build();
    }

    if (locale != null && supportedLocales.contains(locale)) {
      return getLocalizedDescription(code, locale);
    }

    if (locale != null && locale.split("-").length >= 1 && supportedLocales.contains(locale.split("-")[0])) {
      return getLocalizedDescription(code, locale.split("-")[0]);
    }

    return getLocalizedDescription(code, "en-US");
  }

  private Response getLocalizedDescription(String code, String locale) {
    String path = constructUrlForLocale(baseUrl, locale);

    return Response.ok()
                   .entity(String.format(PLAY_TWIML,
                                         path + "verification.mp3",
                                         path + code.charAt(0) + "_middle.mp3",
                                         path + code.charAt(1) + "_middle.mp3",
                                         path + code.charAt(2) + "_middle.mp3",
                                         path + code.charAt(3) + "_middle.mp3",
                                         path + code.charAt(4) + "_middle.mp3",
                                         path + code.charAt(5) + "_falling.mp3",
                                         path + "verification.mp3",
                                         path + code.charAt(0) + "_middle.mp3",
                                         path + code.charAt(1) + "_middle.mp3",
                                         path + code.charAt(2) + "_middle.mp3",
                                         path + code.charAt(3) + "_middle.mp3",
                                         path + code.charAt(4) + "_middle.mp3",
                                         path + code.charAt(5) + "_falling.mp3",
                                         path + "verification.mp3",
                                         path + code.charAt(0) + "_middle.mp3",
                                         path + code.charAt(1) + "_middle.mp3",
                                         path + code.charAt(2) + "_middle.mp3",
                                         path + code.charAt(3) + "_middle.mp3",
                                         path + code.charAt(4) + "_middle.mp3",
                                         path + code.charAt(5) + "_falling.mp3"))
                   .build();
  }

  private String constructUrlForLocale(String baseUrl, String locale) {
    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }

    return baseUrl + locale + "/";
  }

}
