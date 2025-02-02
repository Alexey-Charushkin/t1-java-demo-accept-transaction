package ru.t1.java.accept_transaction.service;

import java.util.List;

public interface TransactionService {
    void  checkTransactions(List messageList);

    <T> T registerTransaction(String topic, T transaction);
}
