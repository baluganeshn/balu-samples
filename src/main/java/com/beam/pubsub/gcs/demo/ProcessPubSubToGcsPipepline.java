package com.beam.pubsub.gcs.demo;


import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.WindowedContext;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.repackaged.org.joda.time.Instant;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author Balu.Nair
 * 
 * This is a dataflow pipeline which is constructed based on Apache Beam 2.0.0. The intent of this pipeline is to read from a google pub/sub topic. 
 * The data is then pushed to the Google Cloud Data Storage. New files will be created for each set of message based on the current time stamp. 
 * 
 * Running this pipeline :
 * 
 * From eclipse : 
 * 	1. Create a new Dataflow project. 
 *  2. Add beam 2.0.0 in the maven dependency
 *  3. Copy this file in the project folder and udpate the package names 
 *  4. right click on the file and run as 'Dataflow pipeline'
 *  5. Go to google cloud instance and check the pipeline graph under Dataflow. 
 *  6. Publish any message to the PUB/SUB
 *  7. After a while, check the google cloud storage (GCS) for the file
 *  
 *  Running using Maven:
 *  
 *  
 * 
 *
 */
public class ProcessPubSubToGcsPipepline {

    private static final String CONSTANT_KEY = "constant";

	private static final String CLOUD_PROJECT_ID = "projectId";

	private static final String CLOUD_PUB_SUB_TOPIC = "gs://pubsubdataflow/topicfile";

	private static final String PUBSUB_STAGING_LOCATION = "gs://pubsubdataflow/pubsubstaging";

	private static final String JOB_NAME = "ProcessPubSubToGcsPipepline";

	private static final Logger LOG = LoggerFactory.getLogger(ProcessPubSubToGcsPipepline.class);


    public static final int MAX_EVENTS_IN_FILE = 100;

    public static final String PUBSUB_SUBSCRIPTION = "/topics/projectId/pubSubDemo";


    public static void main(String[] args) {
    	/**
    	 * Pipeline is constructed based on the below parameters. 
    	 * Streaming is enabled 
    	 * 
    	 */
    	
    	PipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		((DataflowPipelineOptions) options).setStreaming(true);
		((DataflowPipelineOptions) options).setJobName(JOB_NAME);
		((DataflowPipelineOptions) options).setProject(CLOUD_PROJECT_ID);
		((DataflowPipelineOptions) options).setStagingLocation(PUBSUB_STAGING_LOCATION);
		((DataflowPipelineOptions) options).setRunner(DataflowRunner.class);

		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<String> streamData = pipeline
				.apply(PubsubIO.readStrings().fromTopic(PUBSUB_SUBSCRIPTION));

        
		/**
		 * A DoFn is created to log the message that's been pulled from the publisher. 
		 */
        streamData.apply(ParDo.of(new DoFn<String, String>() {
			/**
			 * serial version id
			 */
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				LOG.info(c.element());
			}
        }));
        
        /**
         * Grouping the the collection with a constant key, so that the Unbounded collection can be emitted together. 
         */
        PCollection<KV<String, String>> keyedStream = streamData
				.apply(WithKeys.of(new SerializableFunction<String, String>() {
					/**
					 * serial version id
					 */
					private static final long serialVersionUID = 1L;

					public String apply(String s) {
						return CONSTANT_KEY;
					}
				}));
        
        
        /**
         * Converting KV<String, String> to String for easy processing
         */
        PCollection<String> streamedData = keyedStream.apply(ParDo.of(new DoFn<KV<String,String>, String>() {
			
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				c.output(c.element().getValue());
			}
        }));
        
        /**
         *  A transform is created for dividing the elements in the PCollection into Windows and trigger the control when those elements are output
         *  
         *  
         */
        PCollection<String> streamedDataWindows = streamedData.apply(Window.<String>into(new GlobalWindows())
        	    .triggering(Repeatedly
            	        .forever(AfterProcessingTime
                	            .pastFirstElementInPane()
                	            .plusDelayOf(Duration.standardSeconds(30))
                	        )).withAllowedLateness(Duration.standardDays(1)).discardingFiredPanes());

        /**
         * Transform to write the messages from Publisher to the google cloud storage with the given file name policy.
         */
        streamedDataWindows.apply(TextIO.write().to(CLOUD_PUB_SUB_TOPIC).withWindowedWrites().withNumShards(1).withFilenamePolicy(new PerWindowFiles()));
        pipeline.run();
    }
    
    /**
     * 
     * @author Balu.Nair
     *
     *File name policy
     */
    public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

    	 private static final String SHARD_TEMPLATE = "";

		private static final String BEAM = "beam";

		private static final String FILE_NAME_REGEX_REPLACEMENT = "-";

		private static final String FILE_NAME_REGEX = "[:.+]";

		private static final String FILE_SUFFIX = "-file";

		/**
         * The formatter used to format the window timestamp for outputting to the filename.
         */
        private static final DateTimeFormatter formatter = ISODateTimeFormat
                .basicDateTimeNoMillis()
                .withZone(DateTimeZone.getDefault());
        
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		/**
		 * The windowed file name policy is created as follows.
		 * File name would be in the following format :
		 * 
		 *  beam-2017-08-07T07-17-01-824Z0-file
		 */
		@Override
		public ResourceId windowedFilename(ResourceId outputDirectory, WindowedContext context, String extension) {
			
			DateTime time = new DateTime();
			
	        String filenamePrefix = BEAM + FILE_NAME_REGEX_REPLACEMENT + time.toString().replaceAll(FILE_NAME_REGEX, FILE_NAME_REGEX_REPLACEMENT) + context.getPaneInfo().getIndex();
	        
			String filename =  DefaultFilenamePolicy.constructName(filenamePrefix,SHARD_TEMPLATE,FILE_SUFFIX,3,3);

			LOG.info("---file name --"+filename);
			
			return outputDirectory.resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
		}


		@Override
		public ResourceId unwindowedFilename(ResourceId outputDirectory, Context context, String extension) {
			throw new UnsupportedOperationException("Unsupported.");
		}
	}
    
}