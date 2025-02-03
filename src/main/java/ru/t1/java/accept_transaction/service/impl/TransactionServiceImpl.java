package ru.t1.java.accept_transaction.service.impl;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import ru.t1.java.accept_transaction.enums.TransactionState;
import ru.t1.java.accept_transaction.kafka.KafkaProducer;
import ru.t1.java.accept_transaction.model.dto.TransactionRequest;
import ru.t1.java.accept_transaction.model.dto.TransactionResponse;
import ru.t1.java.accept_transaction.service.TransactionService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public class TransactionServiceImpl implements TransactionService {

    @Value("${t1.kafka.topic.transaction_result}")
    private String topic;
    @Value("${t1.limits.transaction_limit}")
    private int transactionLimit;
    @Value("${t1.limits.transaction_timeframe}")
    private long transactionTimeframe;
    private final KafkaProducer producer;
    private final Map<String, List<Long>> transactionCounts = new ConcurrentHashMap<>();

    @Override
    public void checkTransactions(List<TransactionRequest> messageList) {

        for (TransactionRequest transactionRequest : messageList) {
            log.info("Получена транзакция: " + transactionRequest);

            String key = transactionRequest.getClientId().toString() + "_" + transactionRequest.getAccountId().toString();

            long currentTimestamp = System.currentTimeMillis();

            transactionCounts.computeIfPresent(key, (k, timestamps) -> {
                timestamps.removeIf(timestamp -> currentTimestamp - timestamp > transactionTimeframe);
                return timestamps;
            });

            transactionCounts.computeIfAbsent(key, k -> new ArrayList<>()).add(currentTimestamp);

            TransactionResponse transactionResponse = TransactionResponse.builder()
                    .accountId(transactionRequest.getAccountId())
                    .transactionId(transactionRequest.getTransactionId())
                    .build();

            if (transactionRequest.getTransactionAmount().compareTo(transactionRequest.getAccountBalance()) > 0) {
                transactionResponse.setState(TransactionState.REJECTED);
                continue;
            }
            if (transactionCounts.get(key).size() > transactionLimit) {
                transactionResponse.setState(TransactionState.BLOCKED);
            } else transactionResponse.setState(TransactionState.ACCEPTED);

            log.error("Проверенные транзакции: " + transactionResponse);
            registerTransaction(topic, transactionResponse);
        }

    }

    @Override
    public TransactionResponse registerTransaction(String topic, TransactionResponse transaction) {

        AtomicReference<TransactionResponse> saved = new AtomicReference<>();

        Message<TransactionResponse> message = MessageBuilder.withPayload(transaction)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
                .build();

        CompletableFuture<SendResult<Object, Object>> future = producer.sendMessage(message);
        future.thenAccept(sendResult -> {
            log.info("Transaction sent successfully to topic: {}", sendResult.getRecordMetadata().topic());
            ProducerRecord<Object, Object> record = sendResult.getProducerRecord();
            log.info("Message key: {}", record.key());
            log.info("Message value: {}", record.value());
            saved.set(transaction);
        }).exceptionally(ex -> {
            log.error("Failed to send transaction: {}", ex.getMessage(), ex);
            throw new RuntimeException("Failed to send transaction", ex);
        });
        future.join();
        return saved.get();
    }
}
