package fr.marouni.apache.beam.common;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.util.stream.StreamSupport;

public class DoFns {
    public static class ExtractWordsFn extends DoFn<String, String> {
      private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
      private final Distribution lineLenDist = Metrics.distribution(
          ExtractWordsFn.class, "lineLenDistro");

      @ProcessElement
      public void processElement(ProcessContext c) {
        lineLenDist.update(c.element().length());
        if (c.element().trim().isEmpty()) {
          emptyLines.inc();
        }

        // Split the line into words.
        String[] words = c.element().split(Utils.ExampleUtils.TOKENIZER_PATTERN);

        // Output each word encountered into the output PCollection.
        for (String word : words) {
          if (!word.isEmpty()) {
            c.output(word);
          }
        }
      }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
      @Override
      public String apply(KV<String, Long> input) {
        return input.getKey() + ": " + input.getValue();
      }
    }

    public static class GetLineLengthFromString extends DoFn<String, KV<Integer, String>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, String>> out) {
            // Use OutputReceiver.output to emit the output element.
            out.output(KV.of(line.length(), line));
        }
    }

    public static class ListSum extends DoFn<KV<Integer, Iterable<String>>, KV<Integer, Integer>> {
        @ProcessElement
        public void processElement(@Element KV<Integer, Iterable<String>> input, OutputReceiver<KV<Integer, Integer>> out) {
            out.output(KV.of(input.getKey(), StreamSupport.stream(input.getValue().spliterator(), false).map(s -> s.length()))e);
        }
    }
}
