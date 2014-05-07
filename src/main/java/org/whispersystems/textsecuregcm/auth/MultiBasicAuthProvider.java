/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.auth;

import com.sun.jersey.api.model.Parameter;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import io.dropwizard.auth.Auth;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.basic.BasicAuthProvider;
import io.dropwizard.auth.basic.BasicCredentials;

public class MultiBasicAuthProvider<T1,T2> implements InjectableProvider<Auth, Parameter> {

  private final BasicAuthProvider<T1> provider1;
  private final BasicAuthProvider<T2> provider2;

  private final Class<?> clazz1;
  private final Class<?> clazz2;

  public MultiBasicAuthProvider(Authenticator<BasicCredentials, T1> authenticator1,
                                Class<?> clazz1,
                                Authenticator<BasicCredentials, T2> authenticator2,
                                Class<?> clazz2,
                                String realm)
  {
    this.provider1 = new BasicAuthProvider<>(authenticator1, realm);
    this.provider2 = new BasicAuthProvider<>(authenticator2, realm);
    this.clazz1    = clazz1;
    this.clazz2    = clazz2;
  }


  @Override
  public ComponentScope getScope() {
    return ComponentScope.PerRequest;
  }

  @Override
  public Injectable<?> getInjectable(ComponentContext componentContext,
                                     Auth auth, Parameter parameter)
  {
    if (parameter.getParameterClass().equals(clazz1)) {
      return this.provider1.getInjectable(componentContext, auth, parameter);
    } else {
      return this.provider2.getInjectable(componentContext, auth, parameter);
    }
  }
}
