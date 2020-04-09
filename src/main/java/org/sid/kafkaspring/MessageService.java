package org.sid.kafkaspring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
  @KafkaListener(topics = "topic5",groupId = "group-id")
  public void  message(ConsumerRecord<String,String> message) throws Exception{
   System.out.println("*************************************");
    PageEvent pageEvent=pageEvent(message.value());
     System.out.println("key =>"+message.key());
    System.out.println(" value =>"+pageEvent.getPage()+","+"Date =>"+pageEvent.getDate()+","
      +" Duration =>"+pageEvent.getDuration());
    System.out.println("*************************************");
  }
  private PageEvent pageEvent(String jsonPageEvent) throws Exception{
    JsonMapper jsonMapper=new JsonMapper();

      PageEvent pageEvent= jsonMapper.readValue(jsonPageEvent,PageEvent.class);
return pageEvent;
  }
}
