package com.inovationtech.example_kafka.service;

import java.util.Random;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;

import com.inovationtech.example_kafka.record.OrderRecord;

@Service
public class OrderService {
  
  private final KafkaTemplate<String, OrderRecord> orderKafkaTemplate;
  private final Random random = new Random();

  public OrderService(KafkaTemplate<String, OrderRecord> orderKafkaTemplate) {
    this.orderKafkaTemplate = orderKafkaTemplate;
  }

  @PostMapping
  public void sendMessageOrder(OrderRecord orderRecord) {
    int partition = random.nextInt(2);
    System.out.println("Envie uma mensagem para a partição: " + partition);
    System.err.println("Enviando o order: " + orderRecord);
    orderKafkaTemplate.send("napoleon-order-processed", partition, null, orderRecord);
  }
}
