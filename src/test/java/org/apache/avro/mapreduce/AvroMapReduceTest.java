package org.apache.avro.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class AvroMapReduceTest {
	@Test
	public void shouldRunMapReduceJobWithNonAvroInput() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = new Configuration();
		Job job = new Job(config);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(WordCountingTextToAvroMapper.class);
		job.setReducerClass(WordCountingAvroReducer.class);
		
		//set map output to String=>Long pair schema
		//this triggers the job to use avro serialization and shuffling
		AvroJob.setMapOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set overall output to String=>Long pair schema
		//this triggers the job to use avro output and shuffling
		AvroJob.setOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set input and output paths
		String outputDirectory = getNonExistingTemporaryDirectoryName();
		FileInputFormat.setInputPaths(job, "src/test/data/textfile.txt");
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
		
		if (!job.waitForCompletion(false)) {
			fail("Could not successfully run text to avro job.");
		}
		
		//did it work?
		assertCorrectReduceResults(outputDirectory);
		
		//clean up after ourselves
		FileUtils.deleteDirectory(new File(outputDirectory));
	}
	
	@Test
	public void shouldRunMapperOnlyJobWithNonAvroInput() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = new Configuration();
		Job job = new Job(config);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(WordCountingTextToAvroMapperOnlyMapper.class);
		
		//mapper only job!
		job.setNumReduceTasks(0);
		
		//set map output to String=>Long pair schema
		//this triggers the job to use avro serialization and shuffling
		AvroJob.setMapOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set overall output to String=>Long pair schema
		//this triggers the job to use avro output and shuffling
		AvroJob.setOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());

		//set input and output paths
		String outputDirectory = getNonExistingTemporaryDirectoryName();
		FileInputFormat.setInputPaths(job, "src/test/data/textfile.txt");
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
		
		if (!job.waitForCompletion(false)) {
			fail("Could not successfully run text to avro job.");
		}
		
		//did it work?
		assertCorrectMapResults(outputDirectory);
		
		//clean up after ourselves
		FileUtils.deleteDirectory(new File(outputDirectory));
	}

	@Test
	public void shouldRunMapperOnlyJobWithAvroInput() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = new Configuration();
		Job job = new Job(config);
		
		//Use identity mapper
		job.setMapperClass(Mapper.class);
		
		//mapper only job!
		job.setNumReduceTasks(0);
		
		//set input to String=>Long pair schema
		//this triggers Avro input format
		AvroJob.setInputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set map output to String=>Long pair schema
		//this triggers the job to use avro serialization and shuffling
		AvroJob.setMapOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set overall output to String=>Long pair schema
		//this triggers the job to use avro output and shuffling
		AvroJob.setOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());

		//set input and output paths
		String outputDirectory = getNonExistingTemporaryDirectoryName();
		FileInputFormat.setInputPaths(job, "src/test/data/map-output.avro"); //Avro file!
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
		
		if (!job.waitForCompletion(false)) {
			fail("Could not successfully run text to avro job.");
		}
		
		//did it work?
		assertCorrectMapResults(outputDirectory);
		
		//clean up after ourselves
		FileUtils.deleteDirectory(new File(outputDirectory));
	}

	@Test
	public void shouldRunMapReduceJobWithAvroInput() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration config = new Configuration();
		Job job = new Job(config);
		
		job.setMapperClass(IdentityAvroMapper.class);
		job.setReducerClass(WordCountingAvroReducer.class);
		
		//set input to String=>Long pair schema
		//this triggers Avro input format
		AvroJob.setInputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set map output to String=>Long pair schema
		//this triggers the job to use avro serialization and shuffling
		AvroJob.setMapOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		
		//set overall output to String=>Long pair schema
		//this triggers the job to use avro output and shuffling
		AvroJob.setOutputSchema(job, new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());

		//set input and output paths
		String outputDirectory = getNonExistingTemporaryDirectoryName();
		FileInputFormat.setInputPaths(job, "src/test/data/map-output.avro"); //Avro file!
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
		
		if (!job.waitForCompletion(false)) {
			fail("Could not successfully run text to avro job.");
		}
		
		//did it work?
		assertCorrectReduceResults(outputDirectory);
		
		//clean up after ourselves
		FileUtils.deleteDirectory(new File(outputDirectory));
	}
	
	/*
	 * There is no generic Avro identity mapper or reducer. You could create one
	 * by assuming that the input for mapper is always a pair schema and output
	 * of reducer is also always a pair schema (which is actually is, just
	 * didn't bother)
	 */
	public static class IdentityAvroMapper extends AvroMapper<Pair<Utf8,Long>, Utf8, Long> {
		@Override
		protected void doMap(Pair<Utf8, Long> datum, Context context) throws IOException, InterruptedException {
			writePair(context, datum.key(), datum.value());
		}
	}
		
	public static class WordCountingTextToAvroMapper extends Mapper<LongWritable, Text, AvroKey<Utf8>, AvroValue<Long>> {
		private AvroKey<Utf8> outputKey = new AvroKey<Utf8>();
		private AvroValue<Long> outputValue = new AvroValue<Long>();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String word : value.toString().split("[\\s,\\.]+")) {
				outputKey.datum(new Utf8(word));
				outputValue.datum(1L);
				context.write(outputKey, outputValue);
			}
		}
	}
	
	public static class WordCountingTextToAvroMapperOnlyMapper extends Mapper<LongWritable, Text, AvroWrapper<Pair<Utf8, Long>>, NullWritable> {
		private AvroWrapper<Pair<Utf8, Long>> outputKey = new AvroWrapper<Pair<Utf8,Long>>();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String word : value.toString().split("[\\s,\\.]+")) {
				outputKey.datum(new Pair<Utf8, Long>(new Utf8(word), 1L));
				context.write(outputKey, NullWritable.get());
			}
		}
	}
	
	public static class WordCountingAvroReducer extends AvroReducer<Utf8, Long, Pair<Utf8, Long>> {
		protected void doReduce(Utf8 key, Iterable<Long> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (long l : values) {
				sum += l;
			}
			write(context, new Pair<Utf8, Long>(key, sum));
		}
	}
	
	private void assertCorrectReduceResults(String outputDirectory) throws IOException {
		DatumReader<Record> reader = new GenericDatumReader<Record>(new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		DataFileReader<Record> fileReader = new DataFileReader<Record>(new File(outputDirectory + "/part-r-00000.avro"), reader);
		Map<String, Long> resultMap = new HashMap<String, Long>();
		
		for (Record record : fileReader) {
			resultMap.put(((Utf8) record.get("key")).toString(), (Long) record.get("value"));
		}
		
		verifyResultMap(resultMap);
	}

	private void assertCorrectMapResults(String outputDirectory) throws IOException {
		DatumReader<Record> reader = new GenericDatumReader<Record>(new Pair<Utf8,Long>(new Utf8(), 1L).getSchema());
		DataFileReader<Record> fileReader = new DataFileReader<Record>(new File(outputDirectory + "/part-m-00000.avro"), reader);
		Map<String, Long> resultMap = new HashMap<String, Long>();
		
		//reduce locally
		for (Record record : fileReader) {
			String key = ((Utf8) record.get("key")).toString();
			long sum = resultMap.containsKey(key) ? resultMap.get(key) : 0L;
			sum += (Long) record.get("value");
			
			resultMap.put(key, sum);
		}
		
		verifyResultMap(resultMap);
	}
	
	private void verifyResultMap(Map<String, Long> resultMap) {
		assertEquals(91, resultMap.size());
		assertEquals(Long.valueOf(2L), resultMap.get("And"));
		assertEquals(Long.valueOf(1L), resultMap.get("Better"));
		assertEquals(Long.valueOf(1L), resultMap.get("By"));
		assertEquals(Long.valueOf(1L), resultMap.get("Don't"));
		assertEquals(Long.valueOf(1L), resultMap.get("For"));
		assertEquals(Long.valueOf(5L), resultMap.get("Hey"));
		assertEquals(Long.valueOf(13L), resultMap.get("Jude"));
		assertEquals(Long.valueOf(8L), resultMap.get("Na"));
		assertEquals(Long.valueOf(6L), resultMap.get("Na-na"));
		assertEquals(Long.valueOf(3L), resultMap.get("Remember"));
		assertEquals(Long.valueOf(1L), resultMap.get("So"));
		assertEquals(Long.valueOf(2L), resultMap.get("Take"));
		assertEquals(Long.valueOf(2L), resultMap.get("The"));
		assertEquals(Long.valueOf(4L), resultMap.get("Then"));
		assertEquals(Long.valueOf(2L), resultMap.get("You"));
		assertEquals(Long.valueOf(1L), resultMap.get("You're"));
		assertEquals(Long.valueOf(4L), resultMap.get("a"));
		assertEquals(Long.valueOf(1L), resultMap.get("afraid"));
		assertEquals(Long.valueOf(5L), resultMap.get("and"));
		assertEquals(Long.valueOf(1L), resultMap.get("anytime"));
		assertEquals(Long.valueOf(2L), resultMap.get("bad"));
		assertEquals(Long.valueOf(1L), resultMap.get("be"));
		assertEquals(Long.valueOf(3L), resultMap.get("begin"));
		assertEquals(Long.valueOf(10L), resultMap.get("better"));
		assertEquals(Long.valueOf(2L), resultMap.get("can"));
		assertEquals(Long.valueOf(1L), resultMap.get("carry"));
		assertEquals(Long.valueOf(1L), resultMap.get("colder"));
		assertEquals(Long.valueOf(1L), resultMap.get("cool"));
		assertEquals(Long.valueOf(1L), resultMap.get("do"));
		assertEquals(Long.valueOf(5L), resultMap.get("don't"));
		assertEquals(Long.valueOf(1L), resultMap.get("down"));
		assertEquals(Long.valueOf(1L), resultMap.get("feel"));
		assertEquals(Long.valueOf(1L), resultMap.get("fool"));
		assertEquals(Long.valueOf(1L), resultMap.get("for"));
		assertEquals(Long.valueOf(1L), resultMap.get("found"));
		assertEquals(Long.valueOf(2L), resultMap.get("get"));
		assertEquals(Long.valueOf(2L), resultMap.get("go"));
		assertEquals(Long.valueOf(1L), resultMap.get("have"));
		assertEquals(Long.valueOf(2L), resultMap.get("heart"));
		assertEquals(Long.valueOf(7L), resultMap.get("her"));
		assertEquals(Long.valueOf(8L), resultMap.get("hey"));
		assertEquals(Long.valueOf(1L), resultMap.get("his"));
		assertEquals(Long.valueOf(1L), resultMap.get("in"));
		assertEquals(Long.valueOf(2L), resultMap.get("into"));
		assertEquals(Long.valueOf(1L), resultMap.get("is"));
		assertEquals(Long.valueOf(11L), resultMap.get("it"));
		assertEquals(Long.valueOf(2L), resultMap.get("it's"));
		assertEquals(Long.valueOf(1L), resultMap.get("just"));
		assertEquals(Long.valueOf(2L), resultMap.get("know"));
		assertEquals(Long.valueOf(7L), resultMap.get("let"));
		assertEquals(Long.valueOf(1L), resultMap.get("little"));
		assertEquals(Long.valueOf(1L), resultMap.get("made"));
		assertEquals(Long.valueOf(8L), resultMap.get("make"));
		assertEquals(Long.valueOf(1L), resultMap.get("making"));
		assertEquals(Long.valueOf(1L), resultMap.get("me"));
		assertEquals(Long.valueOf(1L), resultMap.get("minute"));
		assertEquals(Long.valueOf(1L), resultMap.get("movement"));
		assertEquals(Long.valueOf(52L), resultMap.get("na"));
		assertEquals(Long.valueOf(6L), resultMap.get("na-na"));
		assertEquals(Long.valueOf(1L), resultMap.get("need"));
		assertEquals(Long.valueOf(1L), resultMap.get("now"));
		assertEquals(Long.valueOf(1L), resultMap.get("oh!"));
		assertEquals(Long.valueOf(1L), resultMap.get("on"));
		assertEquals(Long.valueOf(2L), resultMap.get("out"));
		assertEquals(Long.valueOf(1L), resultMap.get("pain"));
		assertEquals(Long.valueOf(1L), resultMap.get("perform"));
		assertEquals(Long.valueOf(1L), resultMap.get("plays"));
		assertEquals(Long.valueOf(1L), resultMap.get("refrain"));
		assertEquals(Long.valueOf(2L), resultMap.get("sad"));
		assertEquals(Long.valueOf(1L), resultMap.get("shoulder"));
		assertEquals(Long.valueOf(1L), resultMap.get("shoulders"));
		assertEquals(Long.valueOf(2L), resultMap.get("skin"));
		assertEquals(Long.valueOf(1L), resultMap.get("someone"));
		assertEquals(Long.valueOf(2L), resultMap.get("song"));
		assertEquals(Long.valueOf(2L), resultMap.get("start"));
		assertEquals(Long.valueOf(2L), resultMap.get("that"));
		assertEquals(Long.valueOf(2L), resultMap.get("the"));
		assertEquals(Long.valueOf(9L), resultMap.get("to"));
		assertEquals(Long.valueOf(2L), resultMap.get("under"));
		assertEquals(Long.valueOf(1L), resultMap.get("upon"));
		assertEquals(Long.valueOf(1L), resultMap.get("waiting"));
		assertEquals(Long.valueOf(1L), resultMap.get("well"));
		assertEquals(Long.valueOf(1L), resultMap.get("were"));
		assertEquals(Long.valueOf(1L), resultMap.get("who"));
		assertEquals(Long.valueOf(1L), resultMap.get("with"));
		assertEquals(Long.valueOf(2L), resultMap.get("world"));
		assertEquals(Long.valueOf(1L), resultMap.get("yeah"));
		assertEquals(Long.valueOf(9L), resultMap.get("you"));
		assertEquals(Long.valueOf(1L), resultMap.get("you'll"));
		assertEquals(Long.valueOf(1L), resultMap.get("you?"));
		assertEquals(Long.valueOf(6L), resultMap.get("your"));
	}
	
	private String getNonExistingTemporaryDirectoryName() throws IOException {
		File tempFile = File.createTempFile("avro-mr-temp", "dir");
		if (!tempFile.delete()) {
			fail("Could not delete temp file. Cannot guarantee non-existing temp dir name.");
		}
		
		return tempFile.getAbsolutePath();
	}
}
