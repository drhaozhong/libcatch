package org.apache.cassandra.auth;


import java.util.Map;
import java.util.Set;
import org.apache.cassandra.auth.IAuthenticator.Option;
import org.apache.cassandra.exceptions.ConfigurationException;


public class SimpleAuthenticator implements IAuthenticator {
	public static final String PASSWD_FILENAME_PROPERTY = "passwd.properties";

	public static final String PMODE_PROPERTY = "passwd.mode";

	public static final String USERNAME_KEY = "username";

	public static final String PASSWORD_KEY = "password";

	public enum PasswordMode {

		PLAIN,
		MD5;}

	public AuthenticatedUser defaultUser() {
		return null;
	}

	public void validateConfiguration() throws ConfigurationException {
		String pfilename = System.getProperty(SimpleAuthenticator.PASSWD_FILENAME_PROPERTY);
		if (pfilename == null) {
			throw new ConfigurationException((((("When using " + (this.getClass().getCanonicalName())) + " ") + (SimpleAuthenticator.PASSWD_FILENAME_PROPERTY)) + " properties must be defined."));
		}
	}

	static String authenticationErrorMessage(SimpleAuthenticator.PasswordMode mode, String username) {
		return String.format("Given password in password mode %s could not be validated for user %s", mode, username);
	}

	public void alter(String para0, Map<IAuthenticator.Option, Object> para1) {
		return;
	}

	public void create(String para0, Map<IAuthenticator.Option, Object> para1) {
		return;
	}

	public boolean requireAuthentication() {
		return false;
	}

	public Set<IAuthenticator.Option> alterableOptions() {
		return null;
	}

	public AuthenticatedUser authenticate(Map<String, String> para0) {
		return null;
	}

	public Set<IAuthenticator.Option> supportedOptions() {
		return null;
	}

	public void setup() {
		return;
	}

	public void drop(String para0) {
		return;
	}

	public Set<? extends IResource> protectedResources() {
		return null;
	}
}

