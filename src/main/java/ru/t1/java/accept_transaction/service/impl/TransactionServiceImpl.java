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
import ru.t1.java.accept_transaction.kafka.KafkaProducer;
import ru.t1.java.accept_transaction.service.TransactionService;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public class TransactionServiceImpl  implements TransactionService {

    @Value("${t1.kafka.topic.transaction_result}")
    private String topic;

    private final KafkaProducer producer;

    @Override
    public void checkTransactions(List messageList) {
        log.info("Получена транзакция: " +  messageList.get(0));

    }

    @Override
    public <T> T registerTransaction(String topic, T transaction) {

        AtomicReference<T> saved = new AtomicReference<>();

        Message<T> message = MessageBuilder.withPayload(transaction)
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
