package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


@SuppressWarnings("deprecation")
public class DefaultIteratorEnvironment implements IteratorEnvironment {
	public MyMapFile.Reader reserveMapFileReader(String mapFileName) throws IOException {
		Configuration conf = CachedConfiguration.getInstance();
		FileSystem fs = FileSystem.get(conf);
		return new MyMapFile.Reader(fs, mapFileName, conf);
	}

	public AccumuloConfiguration getConfig() {
		return AccumuloConfiguration.getDefaultConfiguration();
	}

	public IteratorUtil.IteratorScope getIteratorScope() {
		throw new UnsupportedOperationException();
	}

	public boolean isFullMajorCompaction() {
		throw new UnsupportedOperationException();
	}

	public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
		throw new UnsupportedOperationException();
	}
}

