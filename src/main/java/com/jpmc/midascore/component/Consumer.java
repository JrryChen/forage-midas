package com.jpmc.midascore.component;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.web.client.RestTemplate;
import com.jpmc.midascore.foundation.Incentive;

@Component
public class Consumer {

    private static final String INCENTIVE_URL = "http://localhost:8080/incentive";
    private final DatabaseConduit databaseConduit;
    private final UserRepository userRepository;
    private final RestTemplate restTemplate;

    public Consumer(DatabaseConduit databaseConduit, UserRepository userRepository, RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.userRepository = userRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = {"${general.kafka-topic}"})
    public void consume(Transaction T) {
        float amount = T.getAmount();
        long senderId = T.getSenderId();
        long receiverId = T.getRecipientId();

        if (amount < 0) {
            System.out.println("Invalid transaction amount: " + amount);
            return;
        }
        if (senderId == receiverId) {
            System.out.println("Sender and receiver cannot be the same: " + senderId);
            return;
        }
        if (senderId < 0 || receiverId < 0) {
            System.out.println("Invalid sender or receiver ID: " + senderId + ", " + receiverId);
            return;
        }

        UserRecord sender = userRepository.findById(senderId);
        UserRecord receiver = userRepository.findById(receiverId);

        if (sender == null || receiver == null) {
            System.out.println("Sender or receiver does not exist: " + senderId + ", " + receiverId);
            return;
        }

        if (sender.getBalance() < amount) {
            System.out.println("Sender does not have enough balance: " + senderId);
            return;
        }

        // transaction valid, get incentive, update balances and save to database
        Incentive incentive = restTemplate.postForObject(INCENTIVE_URL, T, Incentive.class);
        float incentiveAmount = incentive.getAmount();

        sender.setBalance(sender.getBalance() - amount);
        receiver.setBalance(receiver.getBalance() + amount + incentiveAmount);
        databaseConduit.save(sender);
        databaseConduit.save(receiver);
        databaseConduit.save(new TransactionRecord(sender, receiver, amount, incentiveAmount));

        System.out.println("Processed: " + sender.getName() + " -> " + receiver.getName() + " amount=" + amount + " incentive=" + incentiveAmount);
        System.out.println("Balance update: " + sender.getName() + " = " + sender.getBalance());
        System.out.println("Balance update: " + receiver.getName() + " = " + receiver.getBalance());
    }
}
