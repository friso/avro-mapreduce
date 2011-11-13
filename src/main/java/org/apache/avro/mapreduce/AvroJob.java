package org.apache.avro.mapreduce;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

/** Setters to configure jobs for Avro data. */
public class AvroJob {
	/*
	 * Do not instantiate.
	 */
	private AvroJob() {
	}

	static final String MAPPER = "avro.mapper";
	static final String COMBINER = "avro.combiner";
	static final String REDUCER = "avro.reducer";

	private static final String INPUT_SCHEMA = "avro.input.schema";
	private static final String MAP_OUTPUT_SCHEMA = "avro.map.output.schema";
	private static final String OUTPUT_SCHEMA = "avro.output.schema";
	private static final String OUTPUT_CODEC = "avro.output.codec";
	private static final String TEXT_PREFIX = "avro.meta.text.";
	private static final String BINARY_PREFIX = "avro.meta.binary.";

	/** Configure a job's map input schema. */
	public static void setInputSchema(Job job, Schema schema) {
		job.getConfiguration().set(INPUT_SCHEMA, schema.toString());
		configureAvroInput(job);
	}

	/** Return a job's map input schema. */
	public static Schema getInputSchema(Configuration conf) {
		String schemaString = conf.get(INPUT_SCHEMA);
		return schemaString != null ? new Parser().parse(schemaString) : null;
	}

	/** Configure a job's map output key schema. */
	public static void setMapOutputSchema(Job job, Schema schema) {
		job.getConfiguration().set(MAP_OUTPUT_SCHEMA, schema.toString());
		configureAvroShuffle(job);
	}
	
	/** Return a job's map output key schema. */
	public static Schema getMapOutputSchema(Configuration conf) {
		String schemaString = conf.get(MAP_OUTPUT_SCHEMA);
		return schemaString != null ? new Parser().parse(schemaString) : null;
	}
	
	/** Configure a job's output schema. */
	public static void setOutputSchema(Job job, Schema schema) {
		job.getConfiguration().set(OUTPUT_SCHEMA, schema.toString());
		configureAvroOutput(job);
	}

	/** Return a job's output schema. */
	public static Schema getOutputSchema(Configuration conf) {
		String schemaString = conf.get(OUTPUT_SCHEMA);
		return schemaString != null ? new Parser().parse(schemaString) : null;
	}
	
	/** Configure a job's output compression codec. */
	public static void setOutputCodec(JobConf job, String codec) {
		job.set(OUTPUT_CODEC, codec);
	}

	public static void setOutputMeta(Job job, String key, String value) {
		job.getConfiguration().set(TEXT_PREFIX + key, value);
	}

	public static void setOutputMeta(Job job, String key, long value) {
		job.getConfiguration().setLong(TEXT_PREFIX + key, value);
	}

	public static void setOutputMeta(Job job, String key, byte[] value) {
		try {
			job.getConfiguration().set(BINARY_PREFIX + key, URLEncoder.encode(new String(value, "ISO-8859-1"), "ISO-8859-1"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static void setInputSequenceFile(JobConf job) {
		job.setInputFormat(SequenceFileInputFormat.class);
	}
	
	private static void configureAvroInput(Job job) {
		job.setInputFormatClass(AvroInputFormat.class);

		configureAvroShuffle(job);
	}
	
	private static void configureAvroShuffle(Job job) {
		job.setSortComparatorClass(AvroKeyComparator.class);
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(AvroValue.class);
		
		// add AvroSerialization to io.serializations
		Collection<String> serializations = job.getConfiguration().getStringCollection("io.serializations");
		if (!serializations.contains(AvroSerialization.class.getName())) {
			serializations.add(AvroSerialization.class.getName());
			job.getConfiguration().setStrings("io.serializations", serializations.toArray(new String[0]));
		}
	}
	
	private static void configureAvroOutput(Job job) {
		job.setOutputFormatClass(AvroOutputFormat.class);

		job.setOutputKeyClass(AvroWrapper.class);
		configureAvroShuffle(job);
	}
}
