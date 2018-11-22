/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.marouni.apache.beam.graph;

import fr.marouni.apache.beam.common.GraphVizVisitor;
import fr.marouni.apache.beam.common.Utils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.*;


public class GraphVizTest {

    public static void main(String[] args) throws IOException {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        List<String> LINES = Arrays.asList(
                "AZERTY",
                "QWERTY");


        Create.Values<String> input = Create.of(LINES);

        PCollection<KV<String, Long>> extractedWords = p
                .apply(input)
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(Utils.ExampleUtils.TOKENIZER_PATTERN)) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement());

        // First branch
        extractedWords
                .apply("FormatResults_001", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                })).apply("FormatResults_XXX_001", MapElements.via(new SimpleFunction<String, String>() {
            @Override
            public String apply(String input) {
                return input;
            }
        }));

        // Second branch
        extractedWords
                .apply("FormatResults_002", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                })).apply("FormatResults_XXX_002", MapElements.via(new SimpleFunction<String, String>() {
            @Override
            public String apply(String input) {
                return input;
            }
        }));

        GraphVizVisitor graphVizVisitor = new GraphVizVisitor(p, "/tmp/mypipeviz");
        graphVizVisitor.writeGraph();

        // Run the pipeline.
        p.run().waitUntilFinish();
    }

}
