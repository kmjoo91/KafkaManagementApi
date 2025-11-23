package com.study.kafkamanagement.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaManagementService {
    
    private final AdminClient kafkaAdminClient;
    
    /**
     * Kafka Topic 생성
     * 
     * @param topicName 토픽 이름
     * @param numPartitions 파티션 수
     * @param replicationFactor 복제 팩터
     * @return 생성 성공 여부
     */
    public boolean createTopic(String topicName, int numPartitions, short replicationFactor) {
        try {
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
            CreateTopicsResult result = kafkaAdminClient.createTopics(Collections.singletonList(newTopic));
            
            result.values().get(topicName).get();
            log.info("Topic created successfully: {} with {} partitions and replication factor {}", 
                    topicName, numPartitions, replicationFactor);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.warn("Topic already exists: {}", topicName);
                return false;
            }
            log.error("Failed to create topic: {}", topicName, e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while creating topic: {}", topicName, e);
            return false;
        }
    }
    
    /**
     * Topic의 Partition 수 증가
     * 
     * @param topicName 토픽 이름
     * @param newPartitionCount 새로운 파티션 수
     * @return 변경 성공 여부
     */
    public boolean increasePartitions(String topicName, int newPartitionCount) {
        try {
            // 현재 토픽 정보 조회
            Map<String, TopicDescription> topicDescriptions = 
                    kafkaAdminClient.describeTopics(Collections.singletonList(topicName)).allTopicNames().get();
            
            TopicDescription topicDescription = topicDescriptions.get(topicName);
            if (topicDescription == null) {
                log.error("Topic not found: {}", topicName);
                return false;
            }
            
            int currentPartitionCount = topicDescription.partitions().size();
            
            if (newPartitionCount <= currentPartitionCount) {
                log.warn("New partition count ({}) must be greater than current partition count ({})", 
                        newPartitionCount, currentPartitionCount);
                return false;
            }
            
            // 파티션 수 증가
            Map<String, NewPartitions> newPartitions = Collections.singletonMap(
                    topicName, 
                    NewPartitions.increaseTo(newPartitionCount)
            );
            
            kafkaAdminClient.createPartitions(newPartitions).values().get(topicName).get();
            log.info("Partition count increased for topic {} from {} to {}", 
                    topicName, currentPartitionCount, newPartitionCount);
            return true;
        } catch (ExecutionException e) {
            log.error("Failed to increase partitions for topic: {}", topicName, e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while increasing partitions for topic: {}", topicName, e);
            return false;
        }
    }
    
    /**
     * Topic 정보 조회
     * 
     * @param topicName 토픽 이름
     * @return TopicDescription 또는 null
     */
    public TopicDescription getTopicInfo(String topicName) {
        try {
            Map<String, TopicDescription> topicDescriptions = 
                    kafkaAdminClient.describeTopics(Collections.singletonList(topicName)).allTopicNames().get();
            return topicDescriptions.get(topicName);
        } catch (ExecutionException e) {
            log.error("Failed to get topic info: {}", topicName, e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while getting topic info: {}", topicName, e);
            return null;
        }
    }
    
    /**
     * Kafka Topic 삭제
     * 
     * @param topicName 토픽 이름
     * @return 삭제 성공 여부
     */
    public boolean deleteTopic(String topicName) {
        try {
            kafkaAdminClient.deleteTopics(Collections.singletonList(topicName))
                    .topicNameValues().get(topicName).get();
            log.info("Topic deleted successfully: {}", topicName);
            return true;
        } catch (ExecutionException e) {
            log.error("Failed to delete topic: {}", topicName, e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while deleting topic: {}", topicName, e);
            return false;
        }
    }
}

