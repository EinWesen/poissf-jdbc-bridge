package de.einwesen.jdbc.poissf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DriverTest {

	public DriverTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		
		try {
			
			System.out.println(String.format("%d", 1));
			Class.forName(PoiSSFDriver.class.getName());
			
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
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

		
	}
}
