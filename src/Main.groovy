import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import ru.bankffin.dss.kafka_client.AvroDeserializer
import ru.bankffin.dss.kafka_client.AvroSerializer
import ru.bankffin.dss.kafka_client.config.KafkaServiceConfig
import ru.bankffin.dss.kafka_client.config.properties.KafkaServiceConfigurationProperties
import ru.bankffin.dss.proxy.schema.CreditScheme
import ru.bankffin.dss.proxy.schema.InputMessageRequest
import ru.bankffin.dss.proxy.schema.OutputMessageResponse
import ru.bankffin.dss.proxy.schema.StatedCreditNeed

def kafkaServiceConfigProperities= new KafkaServiceConfigurationProperties();

 class CreditConveyorKafkaServiceConfig extends KafkaServiceConfig {

    private final KafkaServiceConfigurationProperties kafkaServiceConfigurationProperties;

    CreditConveyorKafkaServiceConfig(
            KafkaServiceConfigurationProperties kafkaServiceConfigProperities) {
        this.kafkaServiceConfigurationProperties = kafkaServiceConfigProperities;
    }

    ProducerFactory<String, InputMessageRequest> creditConveyorProducerFactory() {
        Map<String, Object> props = buildProducerProperties(this.kafkaServiceConfigurationProperties,
                StringSerializer.class, AvroSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

     KafkaTemplate<String, InputMessageRequest> creditConveyorKafkaTemplate() {
        return new KafkaTemplate<>(creditConveyorProducerFactory());
    }

     ConsumerFactory<String, OutputMessageResponse> creditConveyorConsumerFactory() {
        Map<String, Object> props = buildConsumerProperties(this.kafkaServiceConfigurationProperties,
                StringDeserializer.class, AvroDeserializer.class);
        final HashMap<String, Object> modifiedProps = new HashMap<>(props);
        modifiedProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        return new DefaultKafkaConsumerFactory<>(
                Collections.unmodifiableMap(modifiedProps),
                new StringDeserializer(), new AvroDeserializer<>(OutputMessageResponse.class));
    }

     ConcurrentKafkaListenerContainerFactory<String, OutputMessageResponse> creditConveyorKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OutputMessageResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(creditConveyorConsumerFactory());
        return factory;
    }
}

println("describe payload");
def kafkaServiceConfigurationProducer = new KafkaServiceConfigurationProperties.Producer();
kafkaServiceConfigurationProducer.setTopic("dss.incoming");
kafkaServiceConfigProperities.setProducer(kafkaServiceConfigurationProducer);
kafkaServiceConfigProperities.setBootstrapServers(List.of("localhost:29092")) ;
def creditConveyorKafkaServiceConfig =  new CreditConveyorKafkaServiceConfig(kafkaServiceConfigProperities);

//def producer = new KafkaProducer<>(props);
def kafkaTemplate =  creditConveyorKafkaServiceConfig.creditConveyorKafkaTemplate();

println("sending....");
def statedCreditNeed = new StatedCreditNeed();
statedCreditNeed.setBasketValue(120000.0);
statedCreditNeed.setAmount(100000.0);
statedCreditNeed.setCurrency('RUB');
statedCreditNeed.setPeriod(36);
statedCreditNeed.setInitialPayment(20000.0);
def creditScheme = new CreditScheme();
creditScheme.code ='test_5d31395c24d1';
creditScheme.minAmount =1000.0;
creditScheme.minAmount =1000.0;
creditScheme.percent =0.25;
statedCreditNeed.setCreditScheme(creditScheme);


def  payload1 = new InputMessageRequest();
payload1.setStatedCreditNeed(statedCreditNeed);
payload1.setMessageId("134");
payload1.setScoreRouteType("POS");

def future =  kafkaTemplate.send("dss.incoming", payload1);
future.get();


println("mess was send")

//producer.close();