package de.MarkusTieger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLHandler {
	
	public static ConfigSQLConnection createConnection(File tmp) throws IOException {
		if(!tmp.exists()) tmp.mkdirs();
		
		File database = new File(tmp, "tmp.db");
		if(!database.exists()) {
			database.createNewFile();
		}
		Connection con = null;
		
		try {
			con = DriverManager.getConnection("jdbc:sqlite:" + database);
		} catch (SQLException e) {
			throw new IOException(e);
		}
		
		return new ConfigSQLConnection(con);
	}

	public static void clean(File tmp) throws IOException {
		File database = new File(tmp, "tmp.db");
		if(database.exists()) Files.delete(database.toPath());
	}

}
