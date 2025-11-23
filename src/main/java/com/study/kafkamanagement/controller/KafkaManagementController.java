package com.study.kafkamanagement.controller;

import com.study.kafkamanagement.dto.CreateTopicRequest;
import com.study.kafkamanagement.dto.IncreasePartitionsRequest;
import com.study.kafkamanagement.dto.TopicInfoResponse;
import com.study.kafkamanagement.service.KafkaManagementService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaManagementController {
    
    private final KafkaManagementService kafkaManagementService;
    
    /**
     * Topic 생성
     */
    @PostMapping("/topics")
    public ResponseEntity<String> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        boolean success = kafkaManagementService.createTopic(
                request.getTopicName(),
                request.getNumPartitions(),
                request.getReplicationFactor()
        );
        
        if (success) {
            return ResponseEntity.ok("Topic created successfully: " + request.getTopicName());
        } else {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Topic creation failed. Topic may already exist: " + request.getTopicName());
        }
    }
    
    /**
     * Topic의 Partition 수 증가
     */
    @PutMapping("/topics/{topicName}/partitions")
    public ResponseEntity<String> increasePartitions(
            @PathVariable String topicName,
            @Valid @RequestBody IncreasePartitionsRequest request) {
        
        boolean success = kafkaManagementService.increasePartitions(
                topicName,
                request.getNewPartitionCount()
        );
        
        if (success) {
            return ResponseEntity.ok("Partition count increased successfully for topic: " + topicName);
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Failed to increase partition count for topic: " + topicName);
        }
    }
    
    /**
     * Topic 정보 조회
     */
    @GetMapping("/topics/{topicName}")
    public ResponseEntity<TopicInfoResponse> getTopicInfo(@PathVariable String topicName) {
        TopicDescription topicDescription = kafkaManagementService.getTopicInfo(topicName);
        
        if (topicDescription == null) {
            return ResponseEntity.notFound().build();
        }
        
        TopicInfoResponse response = TopicInfoResponse.builder()
                .topicName(topicDescription.name())
                .partitionCount(topicDescription.partitions().size())
                .build();
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Topic 삭제
     */
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topicName) {
        boolean success = kafkaManagementService.deleteTopic(topicName);
        
        if (success) {
            return ResponseEntity.ok("Topic deleted successfully: " + topicName);
        } else {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Failed to delete topic: " + topicName);
        }
    }
}

