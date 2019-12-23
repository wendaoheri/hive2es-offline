package org.skyline.tools.es.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * @author sean
 */
@SpringBootApplication
@Configuration
@ComponentScan("org.skyline.tools.es")
@EnableAsync
public class ApplicationEntry {

  public static void main(String[] args) {
    ConfigurableApplicationContext ctx = SpringApplication.run(ApplicationEntry.class, args);

    ctx.registerShutdownHook();

  }
}
