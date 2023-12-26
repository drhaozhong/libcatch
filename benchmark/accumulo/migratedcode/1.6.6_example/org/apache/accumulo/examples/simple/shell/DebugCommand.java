package org.apache.accumulo.examples.simple.shell;


import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;


public class DebugCommand extends Command {
	public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
		Set<String> lines = new TreeSet<String>();
		lines.add("This is a test");
		shellState.printLines(lines.iterator(), true);
		return 0;
	}

	public String description() {
		return "prints a message to test extension feature";
	}

	public int numArgs() {
		return 0;
	}
}

