package fr.marouni.apache.beam.kafka;

import fr.marouni.apache.beam.wordcount.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaCount {
    /**
     * Options supported by {@link WordCount}.
     *
     * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
     * to be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface KafkaCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Default.String("/tmp/beam-tests")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        KafkaCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaCountOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(KafkaIO.<Long, String>read()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("test")
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata()
                )
                .apply(Values.<String>create())
                .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
                //.apply(new CountWords())
                //.apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()).withWindowedWrites().withNumShards(2));

        pipeline.run().waitUntilFinish();
    }
}
