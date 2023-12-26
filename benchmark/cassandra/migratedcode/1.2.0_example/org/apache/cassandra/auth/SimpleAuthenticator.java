package org.apache.cassandra.auth;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Properties;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;


public class SimpleAuthenticator extends LegacyAuthenticator {
	public static final String PASSWD_FILENAME_PROPERTY = "passwd.properties";

	public static final String PMODE_PROPERTY = "passwd.mode";

	public enum PasswordMode {

		PLAIN,
		MD5;}

	public AuthenticatedUser defaultUser() {
		return null;
	}

	public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException {
		String pmode_plain = System.getProperty(SimpleAuthenticator.PMODE_PROPERTY);
		SimpleAuthenticator.PasswordMode mode = SimpleAuthenticator.PasswordMode.PLAIN;
		if (pmode_plain != null) {
			try {
				mode = SimpleAuthenticator.PasswordMode.valueOf(pmode_plain);
			} catch (Exception e) {
				String mode_values = "";
				for (SimpleAuthenticator.PasswordMode pm : SimpleAuthenticator.PasswordMode.values())
					mode_values += ("'" + pm) + "', ";

				mode_values += "or leave it unspecified.";
				throw new AuthenticationException(((("The requested password check mode '" + pmode_plain) + "' is not a valid mode.  Possible values are ") + mode_values));
			}
		}
		String pfilename = System.getProperty(SimpleAuthenticator.PASSWD_FILENAME_PROPERTY);
		String username = credentials.get(IAuthenticator.USERNAME_KEY);
		if (username == null)
			throw new AuthenticationException((("Authentication request was missing the required key '" + (IAuthenticator.USERNAME_KEY)) + "'"));

		String password = credentials.get(IAuthenticator.PASSWORD_KEY);
		if (password == null)
			throw new AuthenticationException((("Authentication request was missing the required key '" + (IAuthenticator.PASSWORD_KEY)) + "'"));

		boolean authenticated = false;
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(pfilename));
			Properties props = new Properties();
			props.load(in);
			if ((props.getProperty(username)) == null)
				throw new AuthenticationException(SimpleAuthenticator.authenticationErrorMessage(mode, username));

			switch (mode) {
				case PLAIN :
					authenticated = password.equals(props.getProperty(username));
					break;
				case MD5 :
					authenticated = MessageDigest.isEqual(FBUtilities.threadLocalMD5Digest().digest(password.getBytes()), Hex.hexToBytes(props.getProperty(username)));
					break;
				default :
					throw new RuntimeException(("Unknown PasswordMode " + mode));
			}
		} catch (IOException e) {
			throw new RuntimeException(((("Authentication table file given by property " + (SimpleAuthenticator.PASSWD_FILENAME_PROPERTY)) + " could not be opened: ") + (e.getMessage())));
		} catch (Exception e) {
			throw new RuntimeException("Unexpected authentication problem", e);
		} finally {
			FileUtils.closeQuietly(in);
		}
		if (!authenticated)
			throw new AuthenticationException(SimpleAuthenticator.authenticationErrorMessage(mode, username));

		return new AuthenticatedUser(username);
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
}

