package ru.t1.java.accept_transaction.service;

import ru.t1.java.accept_transaction.model.dto.TransactionRequest;
import ru.t1.java.accept_transaction.model.dto.TransactionResponse;

import java.util.List;

public interface TransactionService {
    void  checkTransactions(List<TransactionRequest> messageList);

    TransactionResponse registerTransaction(String topic, TransactionResponse transaction);
}
