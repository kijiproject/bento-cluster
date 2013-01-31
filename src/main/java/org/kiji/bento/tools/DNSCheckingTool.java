/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.bento.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * <p>A tool that checks if a machine's configured hostname maps to the same IP address as
 * 'localhost'. This helps identify and advise on a common problem encountered when using Ubuntu.
 * The problem occurs when <code>/etc/hosts</code> contains an entry mapping the canonical hostname
 * <code>localhost</code> to one local IP and the configured hostname to another.
 *
 * <p>For example, a "bad" /etc/hosts file looks like:
 * <code>
 *   127.0.0.1 localhost
 *   127.0.1.1 myHostName.
 * </code>
 * While a "good" /etc/hosts file looks like:
 * <code>
 *   127.0.0.1 localhost myHostName
 * </code>
 *
 * This tool expects one command-line argument, which should be the result of running `hostname` on
 * the machine.
 */
public final class DNSCheckingTool extends Configured implements Tool {

  @Flag(name = "hostname", hidden = true,
      usage = "The hostname of the machine, obtained by running `hostname`")
  private String mHostName = "";

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      System.out.println("Error parsing arguments for DNS check tool.");
      return 1;
    }
    if (mHostName.isEmpty()) {
      System.out.println("You must specify --hostname to run the DNS check tool.");
      return 1;
    }

    boolean unmatchingAddresses = false;
    boolean hostnameNotResolvable = false;
    boolean localhostNotResolvable = false;

    InetAddress hostnameAddress = null;
    InetAddress localhostAddress = null;

    // Can we resolve localhost?
    try {
      localhostAddress = InetAddress.getByName("localhost");
    } catch (UnknownHostException e) {
      // I am not sure if this could ever happen (ie localhost cannot be resolved) but just in
      // case...
      localhostNotResolvable = true;
    }

    // Can we resolve the hostname?
    try {
      hostnameAddress = InetAddress.getByName(mHostName);
    } catch (UnknownHostException e) {
      // Something (probably /etc/hosts) is misconfigured so that the hostname does not resolve to
      // an IP.
      hostnameNotResolvable = true;
    }

    // Do the addresses resolved match?
    if (!hostnameNotResolvable && !localhostNotResolvable) {
      unmatchingAddresses = !localhostAddress.equals(hostnameAddress);
    }

    if (hostnameNotResolvable || localhostNotResolvable || unmatchingAddresses) {
      if (hostnameNotResolvable) {
        System.err.println("Your machine's configured hostname " + mHostName + " cannot be");
        System.err.println("resolved to an IP address. Check /etc/hosts and ensure there is an");
        System.err.println("entry mapping " + mHostName + " to 127.0.0.1. For example:\n");

        System.err.println("127.0.0.1\t" + mHostName + "\n");
      }
      if (localhostNotResolvable) {
        System.err.println("localhost cannot be resolved to an IP address. Check /etc/hosts and");
        System.err.println("ensure there is an entry mapping localhost to 127.0.0.1.");
        System.err.println("For example:\n");

        System.err.println("127.0.0.1\tlocalhost\n");
      }
      if (unmatchingAddresses) {
        System.err.println("localhost and your machine's hostname " + mHostName + " do not");
        System.err.println("resolve to the same IP address (common on Ubuntu machines). Check");
        System.err.println("that /etc/hosts maps both localhost and " + mHostName + " to");
        System.err.println("127.0.0.1.\n");
        System.err.println("For example, /etc/hosts may contain:\n");

        System.err.println("127.0.0.1\tlocalhost");
        System.err.println("127.0.1.1\t" + mHostName + "\n");

        System.err.println("and should be edited to instead contain:\n");
        System.err.println("127.0.0.1\tlocalhost " + mHostName + "\n");
      }
      return 1;
    }
    return 0;
  }

  /**
   * Java program entry point.
   *
   * @param args are the command-line arguments.
   * @throws Exception on error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DNSCheckingTool(), args));
  }
}
