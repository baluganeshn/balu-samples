package com.neudesic.rubicon.demo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.repackaged.runners.core.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gcloud.storage.Storage;
import com.google.gcloud.storage.StorageOptions;
import com.google.gcloud.storage.BlobId;
import com.google.gcloud.storage.BlobInfo;
import com.google.gcloud.WriteChannel;


/*import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;cx
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;*/

public class AVROSubscriber {

	private static final Logger LOG = LoggerFactory.getLogger(AVROSubscriber.class);

	public static final String BUCKET_NAME = "topicfolder/";

	public static final Duration ONE_MIN = Duration.standardMinutes(1);
	public static final Duration ONE_DAY = Duration.standardDays(1);
	public static final Duration TEN_SECONDS = Duration.standardSeconds(10);

	public static void main(String[] args) {
		PipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		// Enforce that this pipeline is always run in streaming mode.
		((DataflowPipelineOptions) options).setStreaming(true);
		((DataflowPipelineOptions) options).setJobName("AVROSubscriber");
		((DataflowPipelineOptions) options).setProject("cloudmigrator-171510");
		((DataflowPipelineOptions) options).setStagingLocation("gs://cloudmigratordataflow/pubsubstaging");
		((DataflowPipelineOptions) options).setRunner(DataflowRunner.class);

		Pipeline pipeline = Pipeline.create(options);
		PCollection<String> details = pipeline
				.apply(PubsubIO.readStrings().fromTopic("/topics/cloudmigrator-171510/sampleTopic"));

		// PCollection<String> details =
		// pipeline.apply(PubsubIO.Read.topic("/topics/cloudmigrator-171510/sampleTopic"));

		// details.apply(TextIO.Write.to("gs://cloudmigratordataflow/pubsub"));

		/*
		 * details.apply(ParDo.of(new DoFn<String, String>() {
		 * 
		 * @ProcessElement public void processElement(ProcessContext c) {
		 * LOG.info(c.element()); String[] elements = c.element().split("\\},");
		 * 
		 * try { for(String el : elements){ LOG.info("**************"+el);
		 * JSONArray docs = new JSONArray(el);
		 * LOG.info("**************"+CDL.toString(docs));
		 * 
		 * } } catch (JSONException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } } }));
		 */

		// PCollection<KV<String, String>> keyedStream =
		PCollection<KV<String, String>> keyedStream = details
				.apply(WithKeys.of(new SerializableFunction<String, String>() {
					public String apply(String s) {
						return "constant";
					}
				}));

		PCollection<KV<String, Iterable<String>>> keyedWindows = keyedStream
				.apply(Window.<KV<String, String>>into(FixedWindows.of(ONE_MIN)).withAllowedLateness(ONE_DAY)
						.triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(10))
								.withLateFirings(AfterFirst.of(AfterPane.elementCountAtLeast(10),
										AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_SECONDS))))
						.discardingFiredPanes())
				.apply(GroupByKey.create());

		PCollection<Iterable<String>> windows = keyedWindows.apply(Values.create());

		/*
		 * ResourceId outputDirectory = LocalResources.fromString("/foo", true);
		 * FilenamePolicy policy =
		 * DefaultFilenamePolicy.constructUsingStandardParameters(
		 * StaticValueProvider.of(outputDirectory),
		 * DefaultFilenamePolicy.DEFAULT_SHARD_TEMPLATE, "");
		 */
		/*
		 * details.apply(TextIO.write().to(
		 * "gs://cloudmigratordataflow/topicfile").withWindowedWrites()
		 * .withFilenamePolicy(policy).withNumShards(4));
		 */

		/*details.apply(TextIO.write().to("gs://cloudmigratordataflow/topicfile").withFilenamePolicy(new PerWindowFiles())
				.withWindowedWrites().withNumShards(3));*/
		
		windows.apply(ParDo.of(new DoGCSWrite()));

		pipeline.run();

	}

	public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

		@Override
		public ResourceId windowedFilename(ResourceId outputDirectory, WindowedContext context, String extension) {
			String filename = "topicfile";
			return outputDirectory.resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
		}

		@Override
		public ResourceId unwindowedFilename(ResourceId outputDirectory, Context context, String extension) {
			throw new UnsupportedOperationException("Unsupported.");
		}
	}

	private static class DoGCSWrite extends DoFn<Iterable<String>, Void> implements RequiresWindowAccess {



        public transient Storage storage;

        { init(); }

        public void init() { storage = StorageOptions.defaultInstance().service(); }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            init();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            long paneIndex = c.pane().getIndex();
            String blobName = "samplefile"+paneIndex;

            BlobId blobId = BlobId.of(BUCKET_NAME, blobName);
            LOG.info("***************"+blobId);

            LOG.info("writing pane {} to blob {}", paneIndex, blobName);
            WriteChannel writer = storage.writer(BlobInfo.builder(blobId).contentType("text/plain").build());
            int i=0;
            for (Iterator<String> it = c.element().iterator(); it.hasNext();) {
              i++;
              LOG.info(it.next().getBytes().toString());
              LOG.info("wrote {} elements to blob {}", i, blobName);
          }
            
//            WriteChannel writer = storage.writer(BlobInfo.builder(blobId).contentType("text/plain").build());
//            LOG.info("blob stream opened for pane {} to blob {} ", paneIndex, blobName);
//            int i=0;
//            for (Iterator<String> it = c.element().iterator(); it.hasNext();) {
//                i++;
//                writer.write(ByteBuffer.wrap(it.next().getBytes()));
//                LOG.info("wrote {} elements to blob {}", i, blobName);
//            }
//            writer.close();
            LOG.info("sucessfully write pane {} to blob {}", paneIndex, blobName);
        }
	}

}
