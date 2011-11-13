package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class AvroMapper<IN, K, V> extends Mapper<AvroWrapper<IN>, NullWritable, AvroKey<K>, AvroValue<V>> {
	protected AvroKey<K> avroKey = new AvroKey<K>();
	protected AvroValue<V> avroValue = new AvroValue<V>();
	
	@Override
	protected void map(AvroWrapper<IN> key, NullWritable value, Context context) throws IOException, InterruptedException {
		doMap(key.datum(), context);
	}
	
	protected abstract void doMap(IN datum, Context context) throws IOException, InterruptedException;
	
	protected void writePair(Context context, K key, V value) throws IOException, InterruptedException {
		context.write(avroKey(key), avroValue(value));
	}
	
	protected AvroKey<K> avroKey(K key) {
		avroKey.datum(key);
		return avroKey;
	}
	
	protected AvroValue<V> avroValue(V val) {
		avroValue.datum(val);
		return avroValue;
	}
}
