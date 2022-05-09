package flink_kafka_integration_demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class FlinkKafkaIntegrationSender {
    public static void main(String[] args) throws Exception {
        String outputTopic = "testtopic_output";
        String server = "localhost:9092";
        streamSender(outputTopic, server);
    }

    public static FlinkKafkaProducer011<String> createStringProducer(
            String topic, String kafkaAddress){

        return new FlinkKafkaProducer011<>(kafkaAddress,
                topic, new SimpleStringSchema());
    }


    public static class StreamGenerator implements SourceFunction<String> {
        boolean flag = true;
        @Override
        public void run(SourceContext<String> context)throws Exception{
            int counter = 0;
            while (flag){
                context.collect("From Flink: " + counter++);
                //System.out.println("From Flink: " + counter++);
                Thread.sleep(1000);
            }
            context.close();
        }
        @Override
        public void cancel(){
            flag = false;
        }
    }

    public static void streamSender(String outputTopic, String server)throws Exception{
        StreamExecutionEnvironment streamExecutionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stringOutputStream =
                streamExecutionEnvironment.addSource(new StreamGenerator());
        FlinkKafkaProducer011<String> flinkKafkaProducer =
                createStringProducer(outputTopic, server);
        stringOutputStream.addSink(flinkKafkaProducer);
        streamExecutionEnvironment.execute();
    }
}
