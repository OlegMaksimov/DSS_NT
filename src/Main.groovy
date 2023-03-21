import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.*
import ru.bankffin.dss.kafka_client.AvroDeserializer
import ru.bankffin.dss.kafka_client.AvroSerializer
import ru.bankffin.dss.kafka_client.config.KafkaServiceConfig
import ru.bankffin.dss.kafka_client.config.properties.KafkaServiceConfigurationProperties
import ru.bankffin.dss.proxy.schema.Address
import ru.bankffin.dss.proxy.schema.Consent
import ru.bankffin.dss.proxy.schema.CreditScheme
import ru.bankffin.dss.proxy.schema.Document
import ru.bankffin.dss.proxy.schema.InputMessageRequest
import ru.bankffin.dss.proxy.schema.OutputMessageResponse
import ru.bankffin.dss.proxy.schema.Person
import ru.bankffin.dss.proxy.schema.PersonAdditionalIncome
import ru.bankffin.dss.proxy.schema.StatedCreditNeed

class CreditConveyorKafkaServiceConfig extends KafkaServiceConfig {

    private final KafkaServiceConfigurationProperties kafkaServiceConfigurationProperties

    CreditConveyorKafkaServiceConfig(
            KafkaServiceConfigurationProperties kafkaServiceConfigProperties) {
        this.kafkaServiceConfigurationProperties = kafkaServiceConfigProperties
    }

    ProducerFactory<String, InputMessageRequest> creditConveyorProducerFactory() {
        Map<String, Object> props = buildProducerProperties(this.kafkaServiceConfigurationProperties,
                StringSerializer.class, AvroSerializer.class)

        return new DefaultKafkaProducerFactory<>(props)
    }

    KafkaTemplate<String, InputMessageRequest> creditConveyorKafkaTemplate() {
        return new KafkaTemplate<>(creditConveyorProducerFactory())
    }

    ConsumerFactory<String, OutputMessageResponse> creditConveyorConsumerFactory() {
        Map<String, Object> props = buildConsumerProperties(this.kafkaServiceConfigurationProperties,
                StringDeserializer.class, AvroDeserializer.class)
        final HashMap<String, Object> modifiedProps = new HashMap<>(props)
        modifiedProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        return new DefaultKafkaConsumerFactory<>(
                Collections.unmodifiableMap(modifiedProps),
                new StringDeserializer(), new AvroDeserializer<>(OutputMessageResponse.class))
    }

    ConcurrentKafkaListenerContainerFactory<String, OutputMessageResponse> creditConveyorKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OutputMessageResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>()
        factory.setConsumerFactory(creditConveyorConsumerFactory())
        return factory
    }
}


println("describe payload")
def kafkaServiceConfigProperities = new KafkaServiceConfigurationProperties()
kafkaServiceConfigProperities.setBootstrapServers(List.of("172.24.7.37:9093"))

//======================================SSL=============================================================================
def ssl = new KafkaServiceConfigurationProperties.Ssl()
ssl.setKeyStoreLocation('C:\\Projects\\dss-tests\\src\\main\\resources\\client-certs-dss\\dss2auto.keystore')
ssl.setKeyStorePassword('T1PQe2j5z4EOTYR')
ssl.setTrustStoreLocation('C:\\Projects\\dss-tests\\src\\main\\resources\\client-certs-dss\\client.truststore')
ssl.setTrustStorePassword('jDbBrwBzzRGx8ik')
kafkaServiceConfigProperities.setSsl(ssl)

//======================================PRODUCER========================================================================
def kafkaServiceConfigurationProducer = new KafkaServiceConfigurationProperties.Producer()
kafkaServiceConfigurationProducer.setTopic("dss.incoming")
kafkaServiceConfigProperities.setProducer(kafkaServiceConfigurationProducer)
def creditConveyorKafkaServiceConfig = new CreditConveyorKafkaServiceConfig(kafkaServiceConfigProperities)

def kafkaTemplate = creditConveyorKafkaServiceConfig.creditConveyorKafkaTemplate()

//======================================PAYLOAD=========================================================================
def statedCreditNeed = new StatedCreditNeed()
statedCreditNeed.setBasketValue(120000.0)
statedCreditNeed.setAmount(100000.0)
statedCreditNeed.setCurrency('RUB')
statedCreditNeed.setPeriod(36)
statedCreditNeed.setInitialPayment(20000.0)

def creditScheme = new CreditScheme()
creditScheme.setCode('test_5d31395c24d1')
creditScheme.setMinAmount(1000.0)
creditScheme.setMaxAmount(400000.0)
creditScheme.setPercent(0.25)
statedCreditNeed.setCreditScheme(creditScheme)

def additionalIncome = new PersonAdditionalIncome()
additionalIncome.setAdditionalIncomeType(7612474480564115185)
additionalIncome.setAdditionalIncomeSum(206798.23176797933)
def additionalIncomes = [additionalIncome]

def person = new Person()
person.setAdditionalIncomes(additionalIncomes)
person.setPersonId(7612474480564115185)
person.setClientBankId(7612474480564115185)
person.setFirstName('Константин')
person.setLastName('Западный')
person.setMiddleName('Тарасович')
person.setDateBirth('06/09/1950')
person.setGender('MALE')
person.setMaritalStatus('466024f5-9df5-4eec-93f7-56b588d3684b')
person.setType(7612474480564115185)
person.setPhoto(null)
person.setPhotoMatch(null)

def document = new Document()
document.setType(21)
document.setSeries('3228')
document.setNumber('658142')
document.setIssueDate('06/22/2016')
document.setIssueOrganizationName('Global Infrastructure Planner')
document.setIssueOrganizationCode('1837064784')
def documents = [document]

def address1 = new Address()
address1.setType(10l)
address1.setKladr('60')
address1.setFias('a45c3909-4e8c-4561-ba39-95e0280ce253')
address1.setCountry('Литва')
address1.setIndex('5ce1ae14-5c51-4a99-9ce0-7c762b1e4718')
address1.setRegion('02')
address1.setCity('Краснодар')
address1.setStreet('Жукова')
address1.setHouse('100')
address1.setBuilding('45020168-1a32-4842-94fe-60e10026d324')
address1.setFlat('10')

def address2 = new Address()
address2.setType(20L)
address2.setKladr('60')
address2.setFias('a45c3909-4e8c-4561-ba39-95e0280ce253')
address2.setCountry('Армения')
address2.setIndex('5ce1ae14-5c51-4a99-9ce0-7c762b1e4718')
address2.setRegion('76')
address2.setCity('Москва')
address2.setStreet('Космонавтов')
address2.setHouse('100')
address2.setBuilding('45020168-1a32-4842-94fe-60e10026d324')
address2.setFlat('10')

def address3 = new Address()
address3.setType(50L)
address3.setKladr('60')
address3.setFias('a45c3909-4e8c-4561-ba39-95e0280ce253')
address3.setCountry('Литва')
address3.setIndex('5ce1ae14-5c51-4a99-9ce0-7c762b1e4718')
address3.setRegion('02')
address3.setCity('Псков')
address3.setStreet('Гагарина')
address3.setHouse('77')
address3.setBuilding('45020168-1a32-4842-94fe-60e10026d324')
address3.setFlat('1')
def addresses = [address1, address2, address3]

def consent1 = new Consent()
consent1.setType('agreePers')
consent1.setStartDate('03/04/2023 - 14:16:50 +0300')
consent1.setEndDate('03/10/2023 - 14:16:50 +0300')

def consent2 = new Consent()
consent2.setType('agreeBKI')
consent2.setStartDate('03/04/2023 - 14:16:50 +0300')
consent2.setEndDate('03/10/2023 - 14:16:50 +0300')

def consent3 = new Consent()
consent3.setType('agreeLiveInsur')
consent3.setStartDate('03/04/2023 - 14:16:50 +0300')
consent3.setEndDate('03/10/2023 - 14:16:50 +0300')
def consents = [consent1, consent2, consent3]

def payload1 = new InputMessageRequest()
payload1.setStatedCreditNeed(statedCreditNeed)
payload1.setExternalRequestId(3519863776035379927)
payload1.setMessageId('214b7415-b15a-4c98-81fb-e05f3b88e657')
payload1.setScoreRouteType('scoring_pos')
payload1.setPerson(person)
payload1.setExternalRequestType(7612474480564115185)
payload1.setExternalStatusId(7612474480564115185)
payload1.setCreatedGroupId('7aeab2f9-87a3-4ddb-9ca7-2fb256b4d7d7')
payload1.setCreatedDate('03/04/2023 - 14:16:50 +0300')
payload1.setDocuments(documents)
payload1.setAddresses(addresses)
payload1.setConsents(consents)

//======================================SENDING=========================================================================
println("sending....")
def future =  kafkaTemplate.send('dss.incoming', payload1)
future.get()

println('mess was send')
