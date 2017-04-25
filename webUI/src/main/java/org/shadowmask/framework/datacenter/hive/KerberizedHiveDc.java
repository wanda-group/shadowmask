/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.framework.datacenter.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * simple hive data center
 */
public class KerberizedHiveDc extends HiveDc {
  private static Logger logger = Logger.getLogger(KerberizedHiveDc.class);
  private String realm;
  private String kdc;
  private String krbUser;
  private String keyTab;
  private String principal;

  public void loginKdc() throws ClassNotFoundException, IOException {
    System.setProperty("java.security.krb5.realm", realm);
    System.setProperty("java.security.krb5.kdc", kdc);
    Configuration conf = new Configuration();
    conf.setBoolean("hadoop.security.authorization", true);
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    Class.forName(getJdbcDriver());
    UserGroupInformation.loginUserFromKeytab(krbUser, keyTab);

  }

  @Override String url() {
    return String
        .format("jdbc:%s://%s:%s/%s;principal=%s", getServerType(), getHost(),
            getPort(), "default", principal);
  }

  public String getKeyTab() {
    return keyTab;
  }

  public void setKeyTab(String keyTab) {
    this.keyTab = keyTab;
  }

  public String getKrbUser() {
    return krbUser;
  }

  public void setKrbUser(String krbUser) {
    this.krbUser = krbUser;
  }

  public String getKdc() {
    return kdc;
  }

  public void setKdc(String kdc) {
    this.kdc = kdc;
  }

  public String getRealm() {
    return realm;
  }

  public void setRealm(String realm) {
    this.realm = realm;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }
}
