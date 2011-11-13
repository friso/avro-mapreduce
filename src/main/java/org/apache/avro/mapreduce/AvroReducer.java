package org.apache.avro.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class AvroReducer<K, V, OUT> extends Reducer<AvroKey<K>, AvroValue<V>, AvroWrapper<OUT>, NullWritable> {
	protected AvroWrapper<OUT> avroWrapper = new AvroWrapper<OUT>();
	
	protected void reduce(AvroKey<K> key, final Iterable<AvroValue<V>> values, Context context) throws IOException, InterruptedException {
		Iterable<V> datumIterator = new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				return new Iterator<V>() {
					private Iterator<AvroValue<V>> iterator = values.iterator();
					
					@Override
					public boolean hasNext() {
						return iterator.hasNext();
					}

					@Override
					public V next() {
						return iterator.next().datum();
					}

					@Override
					public void remove() {
						iterator.remove();
					}
				};
			}
		};
		
		doReduce(key.datum(), datumIterator, context);
	}
	
	protected abstract void doReduce(K key, Iterable<V> values, Context context) throws IOException, InterruptedException ;

	protected void write(Context context, OUT record) throws IOException, InterruptedException {
		context.write(avroWrapped(record), NullWritable.get());
	}

	private AvroWrapper<OUT> avroWrapped(OUT record) {
		avroWrapper.datum(record);
		return avroWrapper;
	}
}
