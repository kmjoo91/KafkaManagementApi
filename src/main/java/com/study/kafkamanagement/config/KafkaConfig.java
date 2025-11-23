package com.study.kafkamanagement.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Getter
public class KafkaConfig {
    
    private final String bootstrapServers;
    private final Security security;
    
    public KafkaConfig(String bootstrapServers, @DefaultValue Security security) {
        this.bootstrapServers = bootstrapServers;
        this.security = security != null ? security : new Security();
    }
    
    @Getter
    public static class Security {
        private final String protocol;
        private final String mechanism;
        private final String username;
        private final String password;
        private final String truststoreLocation;
        private final String truststorePassword;
        private final String keystoreLocation;
        private final String keystorePassword;
        
        public Security() {
            this("PLAINTEXT", null, null, null, null, null, null, null);
        }
        
        public Security(
                @DefaultValue("PLAINTEXT") String protocol,
                String mechanism,
                String username,
                String password,
                String truststoreLocation,
                String truststorePassword,
                String keystoreLocation,
                String keystorePassword) {
            this.protocol = protocol != null ? protocol : "PLAINTEXT";
            this.mechanism = mechanism;
            this.username = username;
            this.password = password;
            this.truststoreLocation = truststoreLocation;
            this.truststorePassword = truststorePassword;
            this.keystoreLocation = keystoreLocation;
            this.keystorePassword = keystorePassword;
        }
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

