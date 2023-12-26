package org.apache.cassandra.auth;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;


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

	public AuthenticatedUser authenticate(Map<? extends CharSequence, ? extends CharSequence> credentials) throws AuthenticationException {
		String pmode_plain = System.getProperty(SimpleAuthenticator.PMODE_PROPERTY);
		SimpleAuthenticator.PasswordMode mode = SimpleAuthenticator.PasswordMode.PLAIN;
		if (null != pmode_plain) {
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
		String username = null;
		CharSequence user = credentials.get(SimpleAuthenticator.USERNAME_KEY);
		if (null == user)
			throw new AuthenticationException((("Authentication request was missing the required key '" + (SimpleAuthenticator.USERNAME_KEY)) + "'"));
		else
			username = user.toString();

		String password = null;
		CharSequence pass = credentials.get(SimpleAuthenticator.PASSWORD_KEY);
		if (null == pass)
			throw new AuthenticationException((("Authentication request was missing the required key '" + (SimpleAuthenticator.PASSWORD_KEY)) + "'"));
		else
			password = pass.toString();

		boolean authenticated = false;
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(pfilename));
			Properties props = new Properties();
			props.load(in);
			if (null == (props.getProperty(username)))
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

