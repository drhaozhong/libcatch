package org.apache.cassandra.bulkloader;


import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilySerializer;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import static org.apache.cassandra.service.StorageService.Verb.BINARY;


public class CassandraBulkLoader {
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Path[] localFiles;

		private JobConf jobconf;

		public void configure(JobConf job) {
			this.jobconf = job;
			String cassConfig;
			try {
				localFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			cassConfig = localFiles[0].getParent().toString();
			System.setProperty("storage-config", cassConfig);
			try {
				StorageService.instance.initClient();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			try {
				Thread.sleep((10 * 1000));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		public void close() {
			try {
				DistributedCache.releaseCache(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), this.jobconf);
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
			try {
				Thread.sleep((3 * 1000));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			StorageService.instance.stopClient();
		}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			ColumnFamily columnFamily;
			String keyspace = "Keyspace1";
			String cfName = "Super1";
			Message message;
			List<ColumnFamily> columnFamilies;
			columnFamilies = new LinkedList<ColumnFamily>();
			String line;
			columnFamily = ColumnFamily.create(keyspace, cfName);
			while (values.hasNext()) {
				line = values.next().toString();
				String[] fields = line.split("\u0001");
				String SuperColumnName = fields[1];
				String ColumnName = fields[2];
				String ColumnValue = fields[3];
				int timestamp = 0;
				columnFamily.addColumn(new QueryPath(cfName, ByteBufferUtil.bytes(SuperColumnName), ByteBufferUtil.bytes(ColumnName)), ByteBufferUtil.bytes(ColumnValue), timestamp);
			} 
			columnFamilies.add(columnFamily);
			message = CassandraBulkLoader.createMessage(keyspace, key.getBytes(), cfName, columnFamilies);
			List<IAsyncResult> results = new ArrayList<IAsyncResult>();
			for (InetAddress endpoint : StorageService.instance.calculateNaturalEndpoints()) {
				results.add(MessagingService.instance().sendRR(message, endpoint));
			}
			for (IAsyncResult result : results) {
				try {
					result.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					throw new RuntimeException(e);
				}
			}
			output.collect(key, new Text(" inserted into Cassandra node(s)"));
		}
	}

	public static void runJob(String[] args) {
		JobConf conf = new JobConf(CassandraBulkLoader.class);
		if ((args.length) >= 4) {
			conf.setNumReduceTasks(new Integer(args[3]));
		}
		try {
			DistributedCache.addCacheFile(new URI("/cassandra/storage-conf.xml#storage-conf.xml"), conf);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setJobName("CassandraBulkLoader_v2");
		conf.setMapperClass(CassandraBulkLoader.Map.class);
		conf.setReducerClass(CassandraBulkLoader.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Message createMessage(String keyspace, byte[] key, String columnFamily, List<ColumnFamily> columnFamilies) {
		ColumnFamily baseColumnFamily;
		DataOutputBuffer bufOut = new DataOutputBuffer();
		RowMutation rm;
		Message message;
		Column column;
		baseColumnFamily = ColumnFamily.create(CFMetaData.getId(keyspace, columnFamily));
		for (ColumnFamily cf : columnFamilies) {
			bufOut.reset();
			ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
			byte[] data = new byte[bufOut.getLength()];
			System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());
			column = new Column(toByteBuffer(cf.id()), ByteBuffer.wrap(data), 0);
			baseColumnFamily.addColumn(column);
		}
		rm = new RowMutation(keyspace, ByteBuffer.wrap(key));
		rm.add(baseColumnFamily);
		try {
			message = rm.makeRowMutationMessage(BINARY, MessagingService.version_);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return message;
	}

	public static void main(String[] args) throws Exception {
		CassandraBulkLoader.runJob(args);
	}
}

