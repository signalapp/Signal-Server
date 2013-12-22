package org.whispersystems.textsecuregcm.tests.entities;

import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.Util;

import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ClientContactTest {

  @Test
  public void serializeToJSON() throws Exception {
    byte[]        token               = Util.getContactToken("+14152222222");
    ClientContact contact             = new ClientContact(token, null, false);
    ClientContact contactWithRelay    = new ClientContact(token, "whisper", false);
    ClientContact contactWithRelaySms = new ClientContact(token, "whisper", true );

    assertThat("Basic Contact Serialization works",
               asJson(contact),
               is(equalTo(jsonFixture("fixtures/contact.json"))));

    assertThat("Contact Relay Serialization works",
               asJson(contactWithRelay),
               is(equalTo(jsonFixture("fixtures/contact.relay.json"))));

    assertThat("Contact Relay+SMS Serialization works",
               asJson(contactWithRelaySms),
               is(equalTo(jsonFixture("fixtures/contact.relay.sms.json"))));
  }

  @Test
  public void deserializeFromJSON() throws Exception {
    ClientContact contact = new ClientContact(Util.getContactToken("+14152222222"),
                                              "whisper", true);

    assertThat("a ClientContact can be deserialized from JSON",
               fromJson(jsonFixture("fixtures/contact.relay.sms.json"), ClientContact.class),
               is(contact));
  }

}
