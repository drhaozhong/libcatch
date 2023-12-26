package org.apache.accumulo.examples.simple.shell;


import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.ShellExtension;


public class MyAppShellExtension extends ShellExtension {
	public String getExtensionName() {
		return "MyApp";
	}

	@Override
	public Shell.Command[] getCommands() {
		return new Shell.Command[]{ new DebugCommand() };
	}
}

