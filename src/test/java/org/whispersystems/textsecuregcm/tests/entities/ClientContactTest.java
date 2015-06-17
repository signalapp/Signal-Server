package org.whispersystems.textsecuregcm.tests.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.Util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.asJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.fromJson;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.jsonFixture;

public class ClientContactTest {

  @Test
  public void serializeToJSON() throws Exception {
    byte[]        token               = Util.getContactToken("+14152222222");
    ClientContact contact             = new ClientContact(token, null);
    ClientContact contactWithRelay    = new ClientContact(token, "whisper");
    ClientContact contactWithRelaySms = new ClientContact(token, "whisper");

    assertThat("Basic Contact Serialization works",
               asJson(contact),
               is(equalTo(jsonFixture("fixtures/contact.json"))));

    assertThat("Contact Relay Serialization works",
               asJson(contactWithRelay),
               is(equalTo(jsonFixture("fixtures/contact.relay.json"))));
  }

  @Test
  public void deserializeFromJSON() throws Exception {
    ClientContact contact = new ClientContact(Util.getContactToken("+14152222222"),
                                              "whisper");

    assertThat("a ClientContact can be deserialized from JSON",
               fromJson(jsonFixture("fixtures/contact.relay.json"), ClientContact.class),
               is(contact));
  }


}
