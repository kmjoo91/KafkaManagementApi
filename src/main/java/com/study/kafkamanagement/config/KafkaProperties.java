package com.study.kafkamanagement.config;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "kafka")
@Getter
public class KafkaProperties {
    
    private final String bootstrapServers;
    private final Security security;
    
    public KafkaProperties(String bootstrapServers, @DefaultValue Security security) {
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
}

