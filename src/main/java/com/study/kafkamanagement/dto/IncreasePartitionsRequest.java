package com.study.kafkamanagement.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class IncreasePartitionsRequest {
    
    @NotNull(message = "New partition count is required")
    @Min(value = 1, message = "New partition count must be at least 1")
    private Integer newPartitionCount;
}

