package org.skyline.tools.es.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class IndexHandler {

  @Autowired
  private ESNodeCompanionService esNodeCompanionService;

  public void handle() {
    log.info("handle index add");
  }
}
