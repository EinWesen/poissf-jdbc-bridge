package de.einwesen.jdbc.poissf;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DriverTest {

	public DriverTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		try {
			
			System.out.println(String.format("%d", 1));
			new PoiSSFDriver();
			
			final String path = "jdbc:poissf:file:///C:/Users/II1437/Desktop/Mappe1.xlsx";
			
			Connection con = DriverManager.getConnection(path);

			ResultSet rs = con.createStatement().executeQuery("SELECT * FROM Tabelle1");
			
			ResultSetMetaData rsm= rs.getMetaData();
			
			System.out.print("| ");
			for (int i=0; i < rsm.getColumnCount(); i++) {
				System.out.print(rsm.getColumnName(i+1));
				System.out.print(" | ");	
			}
			
			System.out.println("");
			System.out.println("########################################");
			while (rs.next()) {
				for (int i=0; i < rsm.getColumnCount(); i++) {
					System.out.println(rs.getString(i+1));
				}
				System.out.println("------------------------------------");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

		
	}
}
