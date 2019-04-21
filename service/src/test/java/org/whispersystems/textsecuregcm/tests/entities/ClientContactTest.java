package org.whispersystems.textsecuregcm.tests.entities;

import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.Util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.whispersystems.textsecuregcm.tests.util.JsonHelpers.*;

public class ClientContactTest {

  @Test
  public void serializeToJSON() throws Exception {
    byte[]        token               = Util.getContactToken("+14152222222");
    ClientContact contact             = new ClientContact(token, null, false, false);
    ClientContact contactWithRelay    = new ClientContact(token, "whisper", false, false);
    ClientContact contactWithRelayVox = new ClientContact(token, "whisper", true, false);
    ClientContact contactWithRelayVid = new ClientContact(token, "whisper", true, true);

    assertThat("Basic Contact Serialization works",
               asJson(contact),
               is(equalTo(jsonFixture("fixtures/contact.json"))));

    assertThat("Contact Relay Serialization works",
               asJson(contactWithRelay),
               is(equalTo(jsonFixture("fixtures/contact.relay.json"))));

    assertThat("Contact Relay Vox Serializaton works",
               asJson(contactWithRelayVox),
               is(equalTo(jsonFixture("fixtures/contact.relay.voice.json"))));

    assertThat("Contact Relay Video Serializaton works",
               asJson(contactWithRelayVid),
               is(equalTo(jsonFixture("fixtures/contact.relay.video.json"))));
  }

  @Test
  public void deserializeFromJSON() throws Exception {
    ClientContact contact = new ClientContact(Util.getContactToken("+14152222222"),
                                              "whisper", false, false);

    assertThat("a ClientContact can be deserialized from JSON",
               fromJson(jsonFixture("fixtures/contact.relay.json"), ClientContact.class),
               is(contact));
  }


}
