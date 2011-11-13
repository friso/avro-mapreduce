package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * A {@link org.apache.hadoop.mapreduce.RecordReader} for Avro container files.
 */
public class AvroRecordReader<T> extends RecordReader<AvroKey<T>, NullWritable> {

	/** An Avro file reader that knows how to seek to record boundries. */
	private DataFileReader<T> reader;
	/** Start of the input split in bytes offset. */
	private long start;
	/** End of the input split in bytes offset. */
	private long end;
	/** Current record from the input split. */
	private AvroKey<T> currentRecord;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		// Create a DataFileReader that can read records out of the input split.
		SeekableInput fileInput = new FsInput(fileSplit.getPath(), context.getConfiguration());
		// We have to get the schema to read the records, which we assume was
		// set in
		// the job's configuration.
		Schema schema = AvroJob.getInputSchema(context.getConfiguration());
		reader = new DataFileReader<T>(fileInput, new SpecificDatumReader<T>(schema));

		// Initialize the start and end offsets into the input file.
		// The reader syncs to the first record after the start of the input
		// split.
		reader.sync(fileSplit.getStart());
		start = reader.previousSync();
		end = fileSplit.getStart() + fileSplit.getLength();

		currentRecord = new AvroKey<T>(null);
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
		return currentRecord;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (float) (reader.previousSync() - start) / (end - start));
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (reader.hasNext() && !reader.pastSync(end)) {
			currentRecord.datum(reader.next(currentRecord.datum()));
			return true;
		}
		return false;
	}
}
