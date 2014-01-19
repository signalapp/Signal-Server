package org.whispersystems.textsecuregcm.tests.entities;

import org.junit.Test;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.util.Util;

import static com.yammer.dropwizard.testing.JsonHelpers.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PreKeyTest {

  @Test
  public void serializeToJSON() throws Exception {
    PreKey preKey = new PreKey(1, "+14152222222", 1, 1234, "test", "identityTest", false);

    assertThat("Basic Contact Serialization works",
               asJson(preKey),
               is(equalTo(jsonFixture("fixtures/prekey.json"))));
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
