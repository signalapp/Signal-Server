package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;


@Path("/v1/keepalive")
public class KeepAliveController {

  @Timed
  @GET
  public Response getKeepAlive() {
    return Response.ok().build();
  }

}
