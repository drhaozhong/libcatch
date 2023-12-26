package org.apache.accumulo.examples.simple.mapreduce.bulk;


import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.io.Text;


public class SetupTable {
	static class Opts extends ClientOnRequiredTable {
		@Parameter(description = "<split> { <split> ... } ")
		List<String> splits = new ArrayList<>();
	}

	public static void main(String[] args) throws Exception {
		SetupTable.Opts opts = new SetupTable.Opts();
		opts.parseArgs(SetupTable.class.getName(), args);
		Connector conn = opts.getConnector();
		conn.tableOperations().create(opts.getTableName());
		if (!(opts.splits.isEmpty())) {
			TreeSet<Text> intialPartitions = new TreeSet<>();
			for (String split : opts.splits) {
				intialPartitions.add(new Text(split));
			}
			conn.tableOperations().addSplits(opts.getTableName(), intialPartitions);
		}
	}
}

