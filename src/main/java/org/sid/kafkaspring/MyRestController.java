package org.sid.kafkaspring;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class MyRestController {
  @Autowired
  private  KafkaTemplate<String,PageEvent> kafkaTemplate;



  @GetMapping("/send/{page}/{topic4}")
  public String send(@PathVariable String page,@PathVariable(name = "topic4") String topic4){
          PageEvent pageEvent =new PageEvent(page,new Date(), new Random().nextInt(1000));
  kafkaTemplate.send(topic4,"key"+pageEvent.getPage(),pageEvent);
      return "Message sent";
  }
}
