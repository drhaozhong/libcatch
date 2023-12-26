package org.apache.cassandra.auth;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;


public class SimpleAuthorizer extends LegacyAuthorizer {
	public static final String ACCESS_FILENAME_PROPERTY = "access.properties";

	public static final String KEYSPACES_WRITE_PROPERTY = "<modify-keyspaces>";

	public EnumSet<Permission> authorize(AuthenticatedUser user, List<Object> resource) {
		if ((((resource.size()) < 2) || (!(Resources.ROOT.equals(resource.get(0))))) || (!(Resources.KEYSPACES.equals(resource.get(1)))))
			return EnumSet.copyOf(Permission.NONE);

		String keyspace;
		String columnFamily = null;
		EnumSet<Permission> authorized = EnumSet.copyOf(Permission.NONE);
		if ((resource.size()) == 2) {
			keyspace = SimpleAuthorizer.KEYSPACES_WRITE_PROPERTY;
			authorized = EnumSet.of(Permission.READ);
		}else
			if ((resource.size()) == 3) {
				keyspace = ((String) (resource.get(2)));
			}else
				if ((resource.size()) == 4) {
					keyspace = ((String) (resource.get(2)));
					columnFamily = ((String) (resource.get(3)));
				}else {
					throw new UnsupportedOperationException();
				}


		String accessFilename = System.getProperty(SimpleAuthorizer.ACCESS_FILENAME_PROPERTY);
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(accessFilename));
			Properties accessProperties = new Properties();
			accessProperties.load(in);
			if (keyspace == (SimpleAuthorizer.KEYSPACES_WRITE_PROPERTY)) {
				String kspAdmins = accessProperties.getProperty(SimpleAuthorizer.KEYSPACES_WRITE_PROPERTY);
				for (String admin : kspAdmins.split(","))
					if (admin.equals(user.username))
						return EnumSet.copyOf(Permission.ALL);


			}
			boolean canRead = false;
			boolean canWrite = false;
			String readers = null;
			String writers = null;
			if (columnFamily == null) {
				readers = accessProperties.getProperty((keyspace + ".<ro>"));
				writers = accessProperties.getProperty((keyspace + ".<rw>"));
			}else {
				readers = accessProperties.getProperty((((keyspace + ".") + columnFamily) + ".<ro>"));
				writers = accessProperties.getProperty((((keyspace + ".") + columnFamily) + ".<rw>"));
			}
			if (readers != null) {
				for (String reader : readers.split(",")) {
					if (reader.equals(user.username)) {
						canRead = true;
						break;
					}
				}
			}
			if (writers != null) {
				for (String writer : writers.split(",")) {
					if (writer.equals(user.username)) {
						canWrite = true;
						break;
					}
				}
			}
			if (canWrite)
				authorized = EnumSet.copyOf(Permission.ALL);
			else
				if (canRead)
					authorized = EnumSet.of(Permission.READ);


		} catch (IOException e) {
			throw new RuntimeException(String.format("Authorization table file '%s' could not be opened: %s", accessFilename, e.getMessage()));
		} finally {
			FileUtils.closeQuietly(in);
		}
		return authorized;
	}

	public void validateConfiguration() throws ConfigurationException {
		String afilename = System.getProperty(SimpleAuthorizer.ACCESS_FILENAME_PROPERTY);
		if (afilename == null) {
			throw new ConfigurationException(String.format("When using %s, '%s' property must be defined.", this.getClass().getCanonicalName(), SimpleAuthorizer.ACCESS_FILENAME_PROPERTY));
		}
	}
}

