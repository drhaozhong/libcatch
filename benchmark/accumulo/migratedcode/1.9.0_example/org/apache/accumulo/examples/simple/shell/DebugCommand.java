package org.apache.accumulo.examples.simple.shell;


import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;


public class DebugCommand extends Shell.Command {
	@Override
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		Set<String> lines = new TreeSet<>();
		lines.add("This is a test");
		shellState.printLines(lines.iterator(), true);
		return 0;
	}

	@Override
	public String description() {
		return "prints a message to test extension feature";
	}

	@Override
	public int numArgs() {
		return 0;
	}
}

