package de.einwesen.jdbc.poissf;


import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class PoiSSFDriver implements Driver {

	/* package-private */ static final String CONNECT_ERROR = "Could not create connection";
	/* package-private */ static final String CONNECTION_IS_CLOSED = "Connection is closed";
	/* package-private */ static final String STATEMENT_IS_CLOSED = "Statement is closed";
	/* package-private */ static final String RESULTSET_IS_CLOSED = "ResultSet is closed";
	/* package-private */ static final String CURSOR_ON_INVALID_ROW = "Cursor is not on a valid row";
	/* package-private */ static final String ERROR_RETRIEVING_DATA = "Error retrieving data from cell";
	/* package-private */ static final String INCOMPATIBLE_DATATYPE = "Incompatible datatype in cell";
	/* package-private */ static final String RESULT_SET_NOT_UPDATETABLE = "ResultSet is not updatetable";
	/* package-private */ static final String INVALID_COLUMN_INDEX = "Invalid columnIndex %d";
	/* package-private */ static final String NOT_IMPLEMENTED_YET = "Not implemented yet";
	/* package-private */ static final String PARAMETER_MAY_NOT_BE_NULL = "parameter %s my not be null";
	
	private static final String URL_PREFIX = "jdbc:poissf:";
	private static final String URL_PATTERN = URL_PREFIX + "file:///.*\\.xls(x|$)";
	
    static{
        try{
            DriverManager.registerDriver(new PoiSSFDriver());
        }catch(SQLException ex){
            throw new ExceptionInInitializerError(ex);
        }
    }

    
	private PoiSSFDriver() {}
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (acceptsURL(url)) {
			try {
				return new PoiSSFConnection(new URL(url.substring(URL_PREFIX.length())));
			} catch (Throwable e) {
				throw new SQLException(CONNECT_ERROR, e);
			}
		} else {
			return null;			
		}
		
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url != null && url.toLowerCase().matches(URL_PATTERN);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 1;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

}
