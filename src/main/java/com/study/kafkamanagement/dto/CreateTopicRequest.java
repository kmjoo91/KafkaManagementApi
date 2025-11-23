package com.study.kafkamanagement.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class CreateTopicRequest {
    
    @NotBlank(message = "Topic name is required")
    private String topicName;
    
    @NotNull(message = "Number of partitions is required")
    @Min(value = 1, message = "Number of partitions must be at least 1")
    private Integer numPartitions;
    
    @NotNull(message = "Replication factor is required")
    @Min(value = 1, message = "Replication factor must be at least 1")
    private Short replicationFactor;
}

