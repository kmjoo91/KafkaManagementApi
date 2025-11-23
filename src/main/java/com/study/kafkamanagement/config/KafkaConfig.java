package com.study.kafkamanagement.config;

import lombok.Data;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaConfig {
    
    private String bootstrapServers;
    private Security security = new Security();
    
    @Data
    public static class Security {
        private String protocol = "PLAINTEXT";
        private String mechanism;
        private String username;
        private String password;
        private String truststoreLocation;
        private String truststorePassword;
        private String keystoreLocation;
        private String keystorePassword;
    }
    
    @Bean
    public AdminClient kafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
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
        
        return AdminClient.create(configs);
    }
}

