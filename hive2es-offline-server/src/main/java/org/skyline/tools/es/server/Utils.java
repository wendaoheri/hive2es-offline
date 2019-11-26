package org.skyline.tools.es.server;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author sean
 */
public class Utils {

  public static String getHostName() throws UnknownHostException {
    InetAddress ia = InetAddress.getLocalHost();
    return ia.getHostName();
  }
  public static String getIp() throws UnknownHostException {
    InetAddress ia = InetAddress.getLocalHost();
    return ia.getHostAddress();
  }
}
