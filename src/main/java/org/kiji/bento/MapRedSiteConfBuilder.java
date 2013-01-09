/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.bento;

/**
 * A builder for a Hadoop XML resource file named "bento-mapred-site.xml" intended to be included
 * in the "mapred-site.xml" file used by bento-cluster. This resource contains property values
 * managed through bento-cluster's configuration utility.
 */
public class MapRedSiteConfBuilder extends ConfigurationBuilder {
  /** The configuration key used for the address of the job tracker. */
  public static final String JOB_TRACKER_ADDRESS_CONF = "mapred.job.tracker";
  /** The configuration key used for the address of the job tracker UI. */
  public static final String JOB_TRACKER_UI_ADDRESS_CONF = "mapred.job.tracker.http.address";

  /** A resource containing a usage comment for the file. */
  private static final String MAPRED_SITE_COMMENT_RESOURCE = "org/kiji/bento/mapred-site-comment";

  /**
   * Constructs a builder.
   */
  public MapRedSiteConfBuilder() {
    super("mapred-site", MAPRED_SITE_COMMENT_RESOURCE);
  }

  /**
   * Uses the specified port for the job tracker in the configuration.
   *
   * @param port The job tracker port.
   * @return This builder, so configuration can be chained.
   */
  public MapRedSiteConfBuilder withJobTrackerPort(int port) {
    return withLocalAddressAtPort(JOB_TRACKER_ADDRESS_CONF, port);
  }

  /**
   * Uses the specified port for the job tracker UI in the configuration.
   *
   * @param port The job tracker UI port.
   * @return This builder, so configuration can be chained.
   */
  public MapRedSiteConfBuilder withJobTrackerUIPort(int port) {
    return withLocalAddressAtPort(JOB_TRACKER_UI_ADDRESS_CONF, port);
  }
}
