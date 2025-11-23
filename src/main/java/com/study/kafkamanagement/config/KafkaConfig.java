package com.study.kafkamanagement.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
@RequiredArgsConstructor
public class KafkaConfig {
    
    private final KafkaProperties kafkaProperties;
    
    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        
        KafkaProperties.Security security = kafkaProperties.getSecurity();
        
        // Security 설정
        if (security.getProtocol() != null && !security.getProtocol().equals("PLAINTEXT")) {
            configs.put("security.protocol", security.getProtocol());
            
            if (security.getMechanism() != null) {
                configs.put("sasl.mechanism", security.getMechanism());
            }
            
            if (security.getUsername() != null && security.getPassword() != null) {
                String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    security.getUsername(), security.getPassword()
                );
                configs.put("sasl.jaas.config", jaasConfig);
            }
            
            if (security.getTruststoreLocation() != null) {
                configs.put("ssl.truststore.location", security.getTruststoreLocation());
                if (security.getTruststorePassword() != null) {
                    configs.put("ssl.truststore.password", security.getTruststorePassword());
                }
            }
            
            if (security.getKeystoreLocation() != null) {
                configs.put("ssl.keystore.location", security.getKeystoreLocation());
                if (security.getKeystorePassword() != null) {
                    configs.put("ssl.keystore.password", security.getKeystorePassword());
                }
            }
        }
        
        AdminClient adminClient = AdminClient.create(configs);
        
        // Kafka 서버 연결 확인
        try {
            log.info("Checking Kafka server connection to: {}", kafkaProperties.getBootstrapServers());
            adminClient.listTopics().listings().get(5, TimeUnit.SECONDS);
            log.info("Successfully connected to Kafka server: {}", kafkaProperties.getBootstrapServers());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            adminClient.close();
            throw new IllegalStateException(
                "Interrupted while connecting to Kafka server: " + kafkaProperties.getBootstrapServers(), e);
        } catch (ExecutionException | TimeoutException e) {
            adminClient.close();
            throw new IllegalStateException(
                "Failed to connect to Kafka server: " + kafkaProperties.getBootstrapServers() + 
                ". Please make sure Kafka server is running.", e);
        }
        
        return adminClient;
    }
}
