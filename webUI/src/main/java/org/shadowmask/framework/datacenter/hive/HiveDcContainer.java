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

import org.apache.log4j.Logger;
import org.shadowmask.jdbc.connection.KerberizedHiveConnectionProvider;
import org.shadowmask.model.datareader.Function;

import java.util.*;

/**
 * container witch contains all data centers .
 */
public class HiveDcContainer {

  private static Logger logger =
      Logger.getLogger(KerberizedHiveConnectionProvider.class);

  private boolean initialized = false;
  /**
   * all hive data centers
   */
  private Map<String, HiveDc> hiveDcMap = new HashMap<>();

  private List<String> allDcNames = new ArrayList<>();

  public synchronized void initFromPropFile(String file) {
    if (initialized) {
      throw new RuntimeException(
          "hive datasource container cannot be " + "initialize twice .");
    }
    ResourceBundle bundle = ResourceBundle.getBundle(file);
    Set<String> dcs = getAllDcNames(bundle);
    for (String dc : dcs) {
      parseDc(dc, bundle);

    }
    initialized = true;
  }

  public HiveDc getDc(String dcName) {
    return hiveDcMap.get(dcName);
  }

  public List<String> getAllDcNames() {
    return allDcNames;
  }

  private void parseDc(String dcName, ResourceBundle bundle) {
    String host = this.getOrNull("hive." + dcName + ".host", bundle);
    Integer port = this.<Integer>getOrNull("hive." + dcName + ".port", bundle,
        new String2Integer());
    String serverType =
        this.getOrNull("hive." + dcName + ".servertype", bundle);
    String authType = this.getOrNull("hive." + dcName + ".auth.method", bundle);

    if (host == null) {
      logger.info("data center : %s skipped because of host is null ");
      return;
    }
    if (port == null) {
      port = 10000;
    }
    if (serverType == null) {
      serverType = "hive2";
    }
    if (authType == null) {
      authType = "simple";
    }

    HiveDc dc = null;
    if ("simple".equalsIgnoreCase(authType)) {
      dc = new SimpleHiveDc();
    } else if ("kerberos".equalsIgnoreCase(authType)) {
      dc = new KerberizedHiveDc();
    } else {
      logger.info(
          "data center %s  skipped, because not authorization method not in [simple,kerberos]");
      return;
    }
    dc.setHost(host);
    dc.setPort(port);
    dc.setServerType(serverType);
    dc.setJdbcDriver(getOrNull("hive." + dcName + ".jdbc.driver", bundle));
    if (dc instanceof SimpleHiveDc) {
      try {
        setUpSimpleHiveDc((SimpleHiveDc) dc, bundle, dcName);
        hiveDcMap.put(dcName, dc);
        allDcNames.add(dcName);
      } catch (Exception e) {
        logger.info(
            "data center %s  skipped, because exception occurred when dc setUp",
            e);
        return;
      }
    } else if (dc instanceof KerberizedHiveDc) {
      try {
        setUpKrbHiveDc((KerberizedHiveDc) dc, bundle, dcName);
        hiveDcMap.put(dcName, dc);
        allDcNames.add(dcName);
      } catch (Exception e) {
        logger.info(
            "data center %s  skipped, because exception occurred when dc setUp",
            e);
        return;
      }
    }
  }

  private void setUpSimpleHiveDc(SimpleHiveDc dc, ResourceBundle bundle,
      String dcName) throws Exception {
    dc.setUsername(getOrNull("hive." + dcName + ".name", bundle));
    dc.setPassowrd(getOrNull("hive." + dcName + ".password", bundle));
    dc.init();
  }

  private void setUpKrbHiveDc(KerberizedHiveDc dc, ResourceBundle bundle,
      String dcName) throws Exception {
    dc.setRealm(getOrNull("hive." + dcName + ".krb.realm", bundle));
    dc.setKdc(getOrNull("hive." + dcName + ".krb.kdc", bundle));
    dc.setKrbUser(getOrNull("hive." + dcName + ".krb.user", bundle));
    dc.setKeyTab(getOrNull("hive." + dcName + ".krb.keytab", bundle));
    dc.setPrincipal(getOrNull("hive." + dcName + ".krb.principal", bundle));
    dc.loginKdc();
  }

  private <T> T getOrNull(String key, ResourceBundle bd,
      Function<String, T> trans) {
    try {
      return trans.apply(bd.getString(key));
    } catch (Exception e) {
      return null;
    }
  }

  private String getOrNull(String key, ResourceBundle bd) {
    try {
      return bd.getString(key);
    } catch (Exception e) {
      return null;
    }
  }

  class String2Integer implements Function<String, Integer> {

    @Override public Integer apply(String s) {
      return Integer.valueOf(s);
    }
  }

  /**
   * get all available data centers .
   *
   * @param bd
   * @return
   */
  private Set<String> getAllDcNames(ResourceBundle bd) {
    Set<String> keys = bd.keySet();
    Set<String> dcs = new HashSet<>();
    for (String key : keys) {
      key = key.toLowerCase();
      //not hive property
      if (!key.startsWith("hive")) {
        continue;
      }
      String[] subs = key.split("\\.");
      //need at list 3 parts
      if (subs.length < 3) {
        continue;
      }
      //the second part must start with dc
      if (subs[1] == null || !subs[1].startsWith("dc")) {
        continue;
      }
      dcs.add(subs[1]);
    }
    return dcs;
  }

}
