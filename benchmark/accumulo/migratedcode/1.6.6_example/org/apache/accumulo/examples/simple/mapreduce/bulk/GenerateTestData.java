package org.apache.accumulo.examples.simple.mapreduce.bulk;


import com.beust.jcommander.Parameter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.accumulo.core.cli.Help;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class GenerateTestData {
	static class Opts extends Help {
		@Parameter(names = "--start-row", required = true)
		int startRow = 0;

		@Parameter(names = "--count", required = true)
		int numRows = 0;

		@Parameter(names = "--output", required = true)
		String outputFile;
	}

	public static void main(String[] args) throws IOException {
		GenerateTestData.Opts opts = new GenerateTestData.Opts();
		opts.parseArgs(GenerateTestData.class.getName(), args);
		FileSystem fs = FileSystem.get(new Configuration());
		PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(opts.outputFile))));
		for (int i = 0; i < (opts.numRows); i++) {
			out.println(String.format("row_%010d\tvalue_%010d", (i + (opts.startRow)), (i + (opts.startRow))));
		}
		out.close();
	}
}

