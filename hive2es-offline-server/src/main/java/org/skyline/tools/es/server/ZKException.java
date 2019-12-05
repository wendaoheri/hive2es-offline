package org.skyline.tools.es.server;

/**
 * @author Sean Liu
 * @date 2019-12-05
 */
public class ZKException extends RuntimeException {

  public ZKException(Exception cause) {
    super(cause);
  }
}
