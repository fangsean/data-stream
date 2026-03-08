package com.datastream.migration.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

/**
 * 线程池配置
 */
@Configuration
public class ThreadPoolConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolConfig.class);
    
    /**
     * 生产者线程池
     * 
     * @param migrationProperties 迁移配置（包含 thread-count 配置）
     */
    @Bean(name = "producerExecutor")
    public ExecutorService producerExecutor(MigrationProperties migrationProperties) {
        int corePoolSize = migrationProperties.getThreadCount();
        int maxPoolSize = Math.max(corePoolSize * 2, 10);
        
        logger.info("创建生产者线程池：核心线程数={}, 最大线程数={}", corePoolSize, maxPoolSize);
        
        return new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactory() {
                private int count = 0;

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("Producer-Thread-" + (++count));
                    thread.setDaemon(true);
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()  // 队列满时由调用线程执行
        );
    }
    
    /**
     * Disruptor 消费者线程池（单线程，保证顺序）
     */
    @Bean(name = "consumerExecutor")
    public ExecutorService consumerExecutor(MigrationProperties migrationProperties) {
        logger.info("创建 Disruptor 消费者线程池");
        int corePoolSize = migrationProperties.getThreadCount();
        int maxPoolSize = Math.max(corePoolSize * 2, 10);

        logger.info("创建消费者线程池：核心线程数={}, 最大线程数={}", corePoolSize, maxPoolSize);

        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactory() {
                    private int count = 0;

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("Disruptor-Consumer-Thread-" + (++count));
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()  // 队列满时由调用线程执行
        );
    }
    
    /**
     * 统计定时器线程池
     */
    @Bean(name = "statsScheduler")
    public ScheduledExecutorService statsScheduler() {
        logger.info("创建统计定时器线程池");
        
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("Migration-Stats-Reporter");
            thread.setDaemon(true);
            return thread;
        });
    }
    
    /**
     * 通用业务线程池（用于其他异步任务）
     */
    @Bean(name = "commonExecutor")
    public ExecutorService commonExecutor() {
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        int maxPoolSize = corePoolSize * 2;
        
        logger.info("创建通用业务线程池：核心线程数={}, 最大线程数={}", corePoolSize, maxPoolSize);
        
        return new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(500),
            new ThreadFactory() {
                private int count = 0;
                
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("Common-Thread-" + (++count));
                    thread.setDaemon(true);
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
