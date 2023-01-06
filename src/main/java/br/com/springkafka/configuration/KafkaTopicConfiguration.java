package br.com.springkafka.configuration;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfiguration {

    private final String topic;

    public KafkaTopicConfiguration(@Value("${topic.name}") String topic) {
        this.topic = topic;
    }

    @Bean
    public NewTopic creatTopic() {
        return new NewTopic(topic, 1, (short) 1);
    }
}
