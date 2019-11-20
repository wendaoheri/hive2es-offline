package org.skyline.tools.es.plugin;

import java.util.Collection;
import java.util.Collections;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

/**
 * @author Sean Liu
 * @date 2019-11-20
 */
public class Hive2ESOfflinePlugin extends Plugin {

  @Override
  public String name() {
    return "hive2es-offline";
  }

  @Override
  public String description() {
    return "hive2es offline load plugin";
  }

  @Override
  public Collection<Module> nodeModules() {
    return Collections.singletonList(new AbstractModule() {
      @Override
      protected void configure() {
        binder().bind(Hive2ESOfflineHandler.class).asEagerSingleton();
      }
    });

  }
}
