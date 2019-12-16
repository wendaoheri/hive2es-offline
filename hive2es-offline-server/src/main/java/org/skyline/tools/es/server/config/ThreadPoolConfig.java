package org.skyline.tools.es.server.config;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtilsBean2;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-12-12
 */
@Component
@ConfigurationProperties
@Configuration
@Data
@Slf4j
public class ThreadPoolConfig {

  private Map<String, Map<String, Object>> threadpools;

  @Bean
  public ThreadPoolTaskExecutor processTaskExecutor()
      throws InvocationTargetException, IllegalAccessException {
    return buildExecutor("process");
  }

  private ThreadPoolTaskExecutor buildExecutor(String key)
      throws InvocationTargetException, IllegalAccessException {
    ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
    ThreadPoolProperties props = ThreadPoolProperties.fromMap(this.threadpools.get(key));
    log.info("Init threadpool [{}] with props : {}", key, props);
    Optional.ofNullable(props.getCorePoolSize()).ifPresent(x -> pool.setCorePoolSize(x));
    Optional.ofNullable(props.getMaxPoolSize()).ifPresent(x -> pool.setMaxPoolSize(x));
    Optional.ofNullable(props.getQueueCapacity()).ifPresent(x -> pool.setQueueCapacity(x));
    Optional.ofNullable(props.getAllowCoreThreadTimeOut())
        .ifPresent(x -> pool.setAllowCoreThreadTimeOut(x));
    Optional.ofNullable(props.getThreadNamePrefix()).ifPresent(x -> pool.setThreadNamePrefix(x));
    Optional.ofNullable(props.getThreadGroupName()).ifPresent(x -> pool.setThreadGroupName(x));
    Optional.ofNullable(props.getKeepAliveSeconds()).ifPresent(x -> pool.setKeepAliveSeconds(x));
    Optional.ofNullable(props.getAwaitTerminationSeconds())
        .ifPresent(x -> pool.setAwaitTerminationSeconds(x));
    Optional.ofNullable(props.getDaemon()).ifPresent(x -> pool.setDaemon(x));
    Optional.ofNullable(props.getWaitForTasksToCompleteOnShutdown())
        .ifPresent(x -> pool.setWaitForTasksToCompleteOnShutdown(x));
    log.info("Threadpool [{}] is {}", key, pool);
    return pool;
  }

  @Data
  public static class ThreadPoolProperties {

    private Integer corePoolSize;
    private Integer maxPoolSize;
    private Integer queueCapacity;
    private Boolean allowCoreThreadTimeOut;
    private String threadGroupName;
    private String threadNamePrefix;
    private Integer keepAliveSeconds;
    private Integer awaitTerminationSeconds;
    private Boolean daemon;
    private Boolean waitForTasksToCompleteOnShutdown;

    public static ThreadPoolProperties fromMap(Map<String, Object> map)
        throws InvocationTargetException, IllegalAccessException {
      ThreadPoolProperties props = new ThreadPoolProperties();
      BeanUtilsBean2.getInstance().populate(props, map);
      return props;
    }

  }

}

