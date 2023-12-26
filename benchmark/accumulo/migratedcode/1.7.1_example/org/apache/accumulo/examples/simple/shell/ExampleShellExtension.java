package org.apache.accumulo.examples.simple.shell;


import com.google.auto.service.AutoService;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellExtension;


@AutoService(ShellExtension.class)
public class ExampleShellExtension extends ShellExtension {
	@Override
	public String getExtensionName() {
		return "ExampleShellExtension";
	}

	@Override
	public Shell.Command[] getCommands() {
		return new Shell.Command[]{ new DebugCommand() };
	}
}

