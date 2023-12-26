package org.apache.accumulo.examples.simple.dirlist;


import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.filedata.ChunkCombiner;
import org.apache.accumulo.examples.simple.filedata.FileDataIngest;
import org.apache.hadoop.io.Text;


public class Ingest {
	static final Value nullValue = new Value(new byte[0]);

	public static final String LENGTH_CQ = "length";

	public static final String HIDDEN_CQ = "hidden";

	public static final String EXEC_CQ = "exec";

	public static final String LASTMOD_CQ = "lastmod";

	public static final String HASH_CQ = "md5";

	public static final TypedValueCombiner.Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

	public static Mutation buildMutation(ColumnVisibility cv, String path, boolean isDir, boolean isHidden, boolean canExec, long length, long lastmod, String hash) {
		if (path.equals("/"))
			path = "";

		Mutation m = new Mutation(QueryUtil.getRow(path));
		Text colf = null;
		if (isDir)
			colf = QueryUtil.DIR_COLF;
		else
			colf = new Text(Ingest.encoder.encode(((Long.MAX_VALUE) - lastmod)));

		m.put(colf, new Text(Ingest.LENGTH_CQ), cv, new Value(Long.toString(length).getBytes()));
		m.put(colf, new Text(Ingest.HIDDEN_CQ), cv, new Value(Boolean.toString(isHidden).getBytes()));
		m.put(colf, new Text(Ingest.EXEC_CQ), cv, new Value(Boolean.toString(canExec).getBytes()));
		m.put(colf, new Text(Ingest.LASTMOD_CQ), cv, new Value(Long.toString(lastmod).getBytes()));
		if ((hash != null) && ((hash.length()) > 0))
			m.put(colf, new Text(Ingest.HASH_CQ), cv, new Value(hash.getBytes()));

		return m;
	}

	private static void ingest(File src, ColumnVisibility cv, BatchWriter dirBW, BatchWriter indexBW, FileDataIngest fdi, BatchWriter data) throws Exception {
		String path = null;
		try {
			path = src.getCanonicalPath();
		} catch (IOException e) {
			path = src.getAbsolutePath();
		}
		System.out.println(path);
		String hash = null;
		if (!(src.isDirectory())) {
			try {
				hash = fdi.insertFileData(path, data);
			} catch (Exception e) {
				return;
			}
		}
		dirBW.addMutation(Ingest.buildMutation(cv, path, src.isDirectory(), src.isHidden(), src.canExecute(), src.length(), src.lastModified(), hash));
		Text row = QueryUtil.getForwardIndex(path);
		if (row != null) {
			Text p = new Text(QueryUtil.getRow(path));
			Mutation m = new Mutation(row);
			m.put(QueryUtil.INDEX_COLF, p, cv, Ingest.nullValue);
			indexBW.addMutation(m);
			row = QueryUtil.getReverseIndex(path);
			m = new Mutation(row);
			m.put(QueryUtil.INDEX_COLF, p, cv, Ingest.nullValue);
			indexBW.addMutation(m);
		}
	}

	private static void recurse(File src, ColumnVisibility cv, BatchWriter dirBW, BatchWriter indexBW, FileDataIngest fdi, BatchWriter data) throws Exception {
		Ingest.ingest(src, cv, dirBW, indexBW, fdi, data);
		if (src.isDirectory()) {
			File[] files = src.listFiles();
			if (files == null)
				return;

			for (File child : files) {
				Ingest.recurse(child, cv, dirBW, indexBW, fdi, data);
			}
		}
	}

	static class Opts extends ClientOpts {
		@Parameter(names = "--dirTable", description = "a table to hold the directory information")
		String nameTable = "dirTable";

		@Parameter(names = "--indexTable", description = "an index over the ingested data")
		String indexTable = "indexTable";

		@Parameter(names = "--dataTable", description = "the file data, chunked into parts")
		String dataTable = "dataTable";

		@Parameter(names = "--vis", description = "the visibility to mark the data", converter = ClientOpts.VisibilityConverter.class)
		ColumnVisibility visibility = new ColumnVisibility();

		@Parameter(names = "--chunkSize", description = "the size of chunks when breaking down files")
		int chunkSize = 100000;

		@Parameter(description = "<dir> { <dir> ... }")
		List<String> directories = new ArrayList<>();
	}

	public static void main(String[] args) throws Exception {
		Ingest.Opts opts = new Ingest.Opts();
		BatchWriterOpts bwOpts = new BatchWriterOpts();
		opts.parseArgs(Ingest.class.getName(), args, bwOpts);
		Connector conn = opts.getConnector();
		if (!(conn.tableOperations().exists(opts.nameTable)))
			conn.tableOperations().create(opts.nameTable);

		if (!(conn.tableOperations().exists(opts.indexTable)))
			conn.tableOperations().create(opts.indexTable);

		if (!(conn.tableOperations().exists(opts.dataTable))) {
			conn.tableOperations().create(opts.dataTable);
			conn.tableOperations().attachIterator(opts.dataTable, new IteratorSetting(1, ChunkCombiner.class));
		}
		BatchWriter dirBW = conn.createBatchWriter(opts.nameTable, bwOpts.getBatchWriterConfig());
		BatchWriter indexBW = conn.createBatchWriter(opts.indexTable, bwOpts.getBatchWriterConfig());
		BatchWriter dataBW = conn.createBatchWriter(opts.dataTable, bwOpts.getBatchWriterConfig());
		FileDataIngest fdi = new FileDataIngest(opts.chunkSize, opts.visibility);
		for (String dir : opts.directories) {
			Ingest.recurse(new File(dir), opts.visibility, dirBW, indexBW, fdi, dataBW);
			int slashIndex = -1;
			while ((slashIndex = dir.lastIndexOf("/")) > 0) {
				dir = dir.substring(0, slashIndex);
				Ingest.ingest(new File(dir), opts.visibility, dirBW, indexBW, fdi, dataBW);
			} 
		}
		Ingest.ingest(new File("/"), opts.visibility, dirBW, indexBW, fdi, dataBW);
		dirBW.close();
		indexBW.close();
		dataBW.close();
	}
}

