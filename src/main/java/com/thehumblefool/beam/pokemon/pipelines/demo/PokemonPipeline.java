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
package com.thehumblefool.beam.pokemon.pipelines.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class PokemonPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(PokemonPipeline.class);

    public static void main(String[] args) {
        Pipeline pipe = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        PCollection<String> lines = pipe.apply("read file", TextIO.read().from("/home/TheHumbleFool/Downloads/pokemon.csv"));

        lines
                .apply("mono/dual type mapping", MapElements.via(
                        new SimpleFunction<String, KV<String, String>>() {
                    @Override
                    public KV<String, String> apply(String line) {
                        if (line.split(",")[3].isEmpty()) {
                            return KV.of("mono", line);
                        } else {
                            return KV.of("dual", line);
                        }
                    }
                }))
                .apply("mono/dual count", Count.perKey())
                .apply("mono/dual count print", ParDo.<KV<String, Long>, Void>of(new DoFn<KV<String, Long>, Void>() {
                    @DoFn.ProcessElement
                    public void printCount(@DoFn.Element KV<String, Long> kv) {
                        System.out.println(kv.getKey() + " count is: " + kv.getValue());
                    }
                }));

        PCollection<KV<String, String>> type1map = lines
                .apply("pokemon type mapping", MapElements.via(new SimpleFunction<String, KV<String, String>>() {
                    @Override
                    public KV<String, String> apply(String line) {
                        return KV.of(line.split(",")[2], line);
                    }
                }));

        PCollection<KV<String, String>> type2map = lines
                .apply("dual type extraction", Filter.by(line -> !line.split(",")[3].isEmpty()))
                .apply("pokemon type mapping", MapElements.via(new SimpleFunction<String, KV<String, String>>() {
                    @Override
                    public KV<String, String> apply(String line) {
                        return KV.of(line.split(",")[3], line);
                    }
                }));

        PCollectionList<KV<String, String>> type1And2 = PCollectionList.of(type1map).and(type2map);
        type1And2.apply("flattening", Flatten.pCollections())
                .apply("pokemon type count", Count.perKey())
                .apply("pokemon count print", ParDo.of(new DoFn<KV<String, Long>, Void>() {
                    @DoFn.ProcessElement
                    public void printCount(@DoFn.Element KV<String, Long> kv) {
                        System.out.println(kv.getKey() + " count is: " + kv.getValue());
                    }
                }));
        pipe.run();
    }
}
