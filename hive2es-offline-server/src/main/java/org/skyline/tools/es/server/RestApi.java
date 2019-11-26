package org.skyline.tools.es.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author sean
 */
@RestController
@RequestMapping(value = "/ws/v1/applications")
@Slf4j
public class RestApi {

  @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
  public String test() {
    log.info("test");
    return "test";
  }
}
