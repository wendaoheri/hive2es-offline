package org.skyline.tools.es.server;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAutoConfiguration
@ComponentScan({"org.skyline.tools.es"})
@EnableAsync
public class TestEntry {

}
