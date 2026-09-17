package com.study.distributed.manager;

import com.study.distributed.manager.config.ManagerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.study.distributed.manager")
@EnableConfigurationProperties(ManagerProperties.class)
@EnableScheduling
public class ManagerApplication {
    public static void main(String[] args) { SpringApplication.run(ManagerApplication.class, args); }
}
