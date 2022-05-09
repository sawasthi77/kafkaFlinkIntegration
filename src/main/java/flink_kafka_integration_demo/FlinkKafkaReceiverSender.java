package flink_kafka_integration_demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class FlinkKafkaReceiverSender {
    public static void main(String[] args) throws Exception {
        String server = "localhost:9092";
        String inputTopic = "testtopic";
        String outputTopic = "testtopic_output";
        streamStringOperation(inputTopic, outputTopic, server);
    }

    public static void streamStringOperation(String inputTopic, String outputTopic, String server)throws Exception{
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer011<String> flinkKafkaConsumer =
                createStringConsumer(inputTopic, server);
        FlinkKafkaProducer011<String> flinkKafkaProducer =
                createStringProducer(outputTopic, server);
        DataStream<String> stringInputStream =
                streamExecutionEnvironment.addSource(flinkKafkaConsumer);
        stringInputStream.map(new StringCapitalizer()).addSink(flinkKafkaProducer);
        streamExecutionEnvironment.execute();
    }
    public static class StringCapitalizer implements MapFunction<String, String>{

        @Override
        public String map(String data) throws Exception {
            return data.toUpperCase();
        }
    }

    public static FlinkKafkaConsumer011<String> createStringConsumer(String topic, String kafkaAddress){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaConsumer011<String> consumer =
                new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties);
        return consumer;
    }

    public static FlinkKafkaProducer011<String> createStringProducer(String topic, String kafkaAddress){
        return new FlinkKafkaProducer011(kafkaAddress, topic, new SimpleStringSchema());
    }
}
