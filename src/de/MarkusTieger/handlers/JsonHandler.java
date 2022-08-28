package de.MarkusTieger.handlers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import de.MarkusTieger.obj.SQLTimestamp;

public class JsonHandler {

	private static final Gson GSON = new GsonBuilder().create(),
			GSON_PRETTY = new GsonBuilder().setPrettyPrinting().create();
	
	
	/*public static void load(Connection con, File config) throws IOException {
		
		Map<String, Object> map = null;
		try(FileInputStream fis = new FileInputStream(config)) {
			map = YAML.load(fis);
		}
		
		for(Map.Entry<String, Object> entry : map.entrySet()) {
			
			if(entry.getValue() instanceof List) {
				List<?> l = (List<?>) entry.getValue();
				
				for(int i = 0; i < l.size(); i++) {
					Object obj = l.get(i);
					
					if(!(obj instanceof Map))
						throw new IllegalArgumentException("Invalid Config (Data of " + entry.getKey() + " List-Index: " + i + ")");
					
					Map<?, ?> m = (Map<?, ?>) obj;
					
					List<String> keys = new ArrayList<>();
					List<Object> values = new ArrayList<>();
					
					for(Map.Entry<?, ?> e : m.entrySet()) {
						String key = e.getKey() + "";
						Object value = e.getValue();
						
						keys.add(key);
						
						if(value instanceof SQLTimestamp) {
							SQLTimestamp timestamp = (SQLTimestamp) value;
							value = new Timestamp(timestamp.timestamp);
						}
						
						values.add(value);
					}
					
					String sql = "INSERT INTO `" + entry.getKey() + "`\n";
					sql += "(\n";
					
					for(int k = 0; k < keys.size(); k++) {
						
						if((k + 1) < keys.size()) {
							sql += (keys.get(k) + ",\n");
						} else {
							sql += (keys.get(k) + "\n");
						}
						
					}
					
					sql += ")\n";
					sql += "VALUES\n";
					sql += "(\n";
					for(int k = 0; k < keys.size(); k++) {
						
						if((k + 1) < keys.size()) {
							sql += "?,\n";
						} else {
							sql += "?\n";
						}
						
					}
					sql += ")\n";
					
					try(PreparedStatement statement = con.prepareStatement(sql)) {
						for(int j = 0; j < values.size(); j++) {
							statement.setObject(j + 1, values.get(j));
						}
						statement.executeUpdate();
					} catch (SQLException e1) {
						throw new IOException(e1);
					}
					
					
				}
				
			} else throw new IllegalArgumentException("Invalid Config (Data of " + entry.getKey() + " )");
		}
		
	}*/
	
	public static void save(Connection con, File config, boolean pretty) throws IOException {
		JsonObject data = new JsonObject();
		
		List<String> tables = new ArrayList<>();
		
		try(ResultSet result = con.getMetaData().getTables(null, null, null, null)) {
			
			while(result.next()) {
				String table = result.getString("TABLE_NAME");
				
				tables.add(table);
			}
			
		} catch (SQLException e) {
			throw new IOException(e);
		}
		
		for(String table : tables) {
			
			JsonArray list = new JsonArray();
			
			try (PreparedStatement statement = con.prepareStatement("SELECT * FROM `" + table + "`")) {
				try(ResultSet result = statement.executeQuery()) {
					ResultSetMetaData meta = result.getMetaData();
					int columns = meta.getColumnCount();
					
					while(result.next()) {
						JsonObject map = new JsonObject();
						for(int column = 1; column <= columns; column++) {
							Object value = result.getObject(column);
							
							if(value instanceof Timestamp) {
								Timestamp timestamp = (Timestamp) value;
								value = new SQLTimestamp();
								((SQLTimestamp)value).timestamp = timestamp.getTime();
							}
							
							if(value instanceof Boolean) {
								map.addProperty(meta.getColumnLabel(column), (Boolean) value);
							} else
							if(value instanceof String) {
								map.addProperty(meta.getColumnLabel(column), (String) value);
							} else
							if(value instanceof Character) {
								map.addProperty(meta.getColumnLabel(column), (Character) value);
							} else
							if(value instanceof Number) {
								map.addProperty(meta.getColumnLabel(column), (Number) value);	
							} else throw new RuntimeException("Invalid value for column: " + column + "( " + meta.getColumnLabel(column) + " ) Value: " + value);
						}
						list.add(map);
					}
				}
			} catch (SQLException e) {
				throw new IOException(e);
			}
			
			data.add(table, data);
		}
		
		String content = (pretty ? GSON_PRETTY : GSON).toJson(data);
		try (FileOutputStream fos = new FileOutputStream(config)) {
			fos.write(content.getBytes(StandardCharsets.UTF_8));
			fos.flush();
		}
	}
	
}
