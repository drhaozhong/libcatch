package org.apache.accumulo.examples.simple.shell;


import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.ShellExtension;
import org.apache.accumulo.shell.Shell;


public class MyAppShellExtension extends ShellExtension {
	public String getExtensionName() {
		return "MyApp";
	}

	@Override
	public Shell.Command[] getCommands() {
		return new Command[]{ new DebugCommand() };
	}
}

