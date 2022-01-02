package org.whispersystems.textsecuregcm.storage;

import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.appconfig.AppConfigClientBuilder;

public class NewAppConfigClientBuilder extends  NewDefAppConfBaseClientBuilder<AppConfigClientBuilder, AppConfigClient>
    implements software.amazon.awssdk.services.appconfig.AppConfigClientBuilder {

  @Override
    protected final AppConfigClient buildClient() {
      return new NewAppConfigClient(super.syncClientConfiguration());
    }

}
