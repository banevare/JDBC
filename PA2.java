import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class PA2 {
	static int maxStops = 99; 				  					// resonable number of stops
	public static void main(String[] args) {
		Connection conn = null;               							// Database connection.
		try {
			Class.forName("org.sqlite.JDBC"); // Load the JDBC class.
			conn = DriverManager.getConnection("jdbc:sqlite:pa2.db"); 
			System.out.println("Opened database successfully.");

			Statement stmt            = conn.createStatement();
			String    dropConnected   = "DROP TABLE IF EXISTS connected;";
			String    createConnected = "CREATE TABLE connected AS " 
										+ "SELECT airline, origin, destination, 0 as Stops FROM Flight;";
			
			stmt.execute(dropConnected);
			stmt.execute(createConnected);
	
			for(int i = 0; i < maxStops; i++){
				String delta = "INSERT INTO connected (airline,origin,destination,stops) " 
				+ "SELECT distinct c1.airline, c1.origin, c2.destination, c1.stops + 1" 
				+ " FROM connected c1, connected c2" + " WHERE c1.stops = "+ i + " AND c2.stops = 0 " 
				+ "AND c1.destination = c2.origin" + " AND c1.airline = c2.airline" 
				+ " AND NOT EXISTS (SELECT * FROM connected c3 WHERE c3.origin = c1.origin" 
				+ " AND c3.destination = c2.destination AND c3.airline = c1.airline) AND c1.origin <> c2.destination;"; 
				PreparedStatement insertStmt = conn.prepareStatement(delta);
				int insertedLines = insertStmt.executeUpdate();
				if (insertedLines == 0) break;
			}
			stmt.close();
			conn.close(); 
		} catch (Exception e) {
			throw new RuntimeException("There was a runtime problem!", e);
		} finally {
			try {
				if (conn != null) conn.close();
			} catch (SQLException e) {
				throw new RuntimeException("Cannot close the connection!", e);
			}
		}
	}
}
