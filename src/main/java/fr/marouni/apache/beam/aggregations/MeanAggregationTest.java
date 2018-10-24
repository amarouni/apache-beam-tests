package fr.marouni.apache.beam.aggregations;

import fr.marouni.apache.beam.common.Transforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;

public class MeanAggregationTest {

    public static void main(String[] args) {

        /* PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        List<String> LINES = Arrays.asList(
                "AZERTY",
                "QWERTY",
                "AZERTYQWERTY",
                "QWERTYAZERTY");

        List<KV<Integer, String>> EXPECTED_OUTPUT = Arrays.asList(
                KV.of(6, "AZERTY"),
                KV.of(6, "QWERTY"));

        PCollection<KV<Integer, String>> collected =
                p.apply(Create.of(LINES))
                .apply(new Transforms.KeyAppender())
                .apply(GroupByKey.<Integer, String>create())

        PAssert.that(collected).containsInAnyOrder(EXPECTED_OUTPUT);

        // Run the pipeline.
        p.run().waitUntilFinish(); */
    }
}
