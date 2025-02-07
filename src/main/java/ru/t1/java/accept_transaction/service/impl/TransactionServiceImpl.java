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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<String, List<TransactionRequest>> transactionBuffer = new ConcurrentHashMap<>();

    //  private final List<TransactionResponse> responseBuffer = Collections.synchronizedList(new ArrayList<>());


    @Override
    public void checkTransactions(List<TransactionRequest> messageList) {
        List<TransactionResponse> checkedTransactions = new ArrayList<>();


        for (TransactionRequest transactionRequest : messageList) {
            log.info("Получена транзакция: " + transactionRequest);

            // получили составной ключ
            String key = transactionRequest.getClientId().toString() + "_" + transactionRequest.getAccountId().toString();
            // получили текушее время
            long currentTimestamp = System.currentTimeMillis();

            // удаляем устаревшие записи
            transactionBuffer.computeIfPresent(key, (k, transactions) -> {
                transactions.removeIf(transaction -> currentTimestamp - transaction.getTimestamp().getTime() > transactionTimeframe);
                return transactions;
            });

            // Добавляем новую транзакцию
            transactionBuffer.computeIfAbsent(key, k -> new ArrayList<>()).add(transactionRequest);

            TransactionResponse transactionResponse = TransactionResponse.builder()
                    .accountId(transactionRequest.getAccountId())
                    .transactionId(transactionRequest.getTransactionId())
                    .build();

            if (transactionRequest.getTransactionAmount().compareTo(transactionRequest.getAccountBalance()) > 0) {
                transactionResponse.setState(TransactionState.REJECTED);
                //   sendTransactionResponse(topic, transactionResponse);
                continue;
            }
            if (transactionBuffer.get(key).size() > transactionLimit) {
               // transactionResponse.setState(TransactionState.BLOCKED);
                blockedTransaction(key, checkedTransactions);
            } else transactionResponse.setState(TransactionState.ACCEPTED);

            log.error("Проверенные транзакции: " + transactionResponse);
            checkedTransactions.add(transactionResponse);
            //  sendTransactionResponse(topic, transactionResponse);
        }
        sendTransactionResponse(topic, checkedTransactions);
    }

    private void blockedTransaction(String key, List<TransactionResponse> responses) {

        // Получили список транзакций для блокировки
        List<TransactionRequest> requests = transactionBuffer.get(key);

        // Создаем HashMap для быстрого поиска TransactionRequest по transactionId
        Map<UUID, TransactionRequest> requestMap = new HashMap<>();
        for (TransactionRequest request : requests) {
            requestMap.put(request.getTransactionId(), request);
        }

        // Обновляем состояние транзакций
        for (TransactionResponse response : responses) {
            TransactionRequest request = requestMap.get(response.getTransactionId());
            if (request != null) {
                response.setState(TransactionState.BLOCKED);
            }
        }
    }

    private void sendTransactionResponse(String topic, List<TransactionResponse> transactions) {

        Message<List<TransactionResponse>> message = MessageBuilder.withPayload(transactions)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
                .build();

        CompletableFuture<SendResult<Object, Object>> future = producer.sendMessage(message);
        future.thenAccept(sendResult -> {
            log.info("Transaction sent successfully to topic: {}", sendResult.getRecordMetadata().topic());
            ProducerRecord<Object, Object> record = sendResult.getProducerRecord();
            log.info("Message key: {}", record.key());
            log.info("Message value: {}", record.value());
        }).exceptionally(ex -> {
            log.error("Failed to send transaction: {}", ex.getMessage(), ex);
            throw new RuntimeException("Failed to send transaction", ex);
        });
        future.join();
    }

    // старый вариант
//    private void sendTransactionResponse(String topic, TransactionResponse transaction) {
//
//        Message<TransactionResponse> message = MessageBuilder.withPayload(transaction)
//                .setHeader(KafkaHeaders.TOPIC, topic)
//                .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
//                .build();
//
//        CompletableFuture<SendResult<Object, Object>> future = producer.sendMessage(message);
//        future.thenAccept(sendResult -> {
//            log.info("Transaction sent successfully to topic: {}", sendResult.getRecordMetadata().topic());
//            ProducerRecord<Object, Object> record = sendResult.getProducerRecord();
//            log.info("Message key: {}", record.key());
//            log.info("Message value: {}", record.value());
//        }).exceptionally(ex -> {
//            log.error("Failed to send transaction: {}", ex.getMessage(), ex);
//            throw new RuntimeException("Failed to send transaction", ex);
//        });
//        future.join();
//    }
}
