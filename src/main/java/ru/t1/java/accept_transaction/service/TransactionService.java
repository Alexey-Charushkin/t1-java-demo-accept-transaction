package ru.t1.java.accept_transaction.service;

import ru.t1.java.accept_transaction.model.dto.TransactionRequest;

import java.util.List;

public interface TransactionService {
    void  checkTransactions(List<TransactionRequest> messageList);

    <T> T registerTransaction(String topic, T transaction);
}
