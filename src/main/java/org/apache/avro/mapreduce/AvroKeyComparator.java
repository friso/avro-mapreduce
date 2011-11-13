package org.apache.avro.mapreduce;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;

/** The {@link RawComparator} used by jobs configured with {@link AvroJob}. */
public class AvroKeyComparator<T> extends Configured implements RawComparator<AvroKey<T>> {

  private Schema schema;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      schema = Pair.getKeySchema(AvroJob.getMapOutputSchema(conf));
    }
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return BinaryData.compare(b1, s1, l1, b2, s2, l2, schema);
  }

  public int compare(AvroKey<T> x, AvroKey<T> y) {
    return SpecificData.get().compare(x.datum(), y.datum(), schema);
  }

}
