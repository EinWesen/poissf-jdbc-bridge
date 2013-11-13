poissf-jdbc-bridge
==================

This project is an attempt to expose XLS(X) files via an JDBC api using Apache POI.

Currently only reading of complete sheets is supported. 


If you know JDBC, you should have no Problem using this library. Never the less, here is an example to get you started.

```java
Class.forName("de.einwesen.jdbc.poissf.PoiSSFDriver");

Connection con = null;
ResultSet rs = null;

try {
  // Reads file, using POI
	con = DriverManager.getConnection("jdbc:poissf:file:///path/to/file.xlsx");
	
	// it's, at least currently,  not that important to close the statement.
	// it doesn't hold any resources..
	rs = con.createStatement().executeQuery("SELECT * FROM Sheetname");
	
	// Information about datatypes is not supported, as teh could be different each row
	// but columnName and count works
	ResultSetMetaData rsm = rs.getMetaData();

	while (rs.next()) {
		for (int i=1; i <= rsm.getColumnCount(); i++) {
			// ResultSet.getObject is your best option, but specific getters (getInt, getLong, etc) 
			// should work if the corresponding cell if contains compatible data.
			System.out.println(rsm.getColumnName(i) + ":" + rs.getObject(i));
		}
		System.out.println("-------------------------------------------------");
	}
	
} catch (SQLException e) {
	e.printStackTrace();
} finally {
	if (rs != null) {
		try {
			rs.close(); // Does nothing currently, but thats likely to change in the future
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	if (con != null) {
		try {
			con.close(); // Removes Workbook from Memory
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
```
