package de.einwesen.jdbc.poissf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class PoiSSFConnection implements Connection {

	
	private /*XSSF*/Workbook excelWorkbook = null;
	private boolean readOnly = true;
	private boolean resultExtendedMetadataEnabled = false;
	private URL workbookURL = null;
	
	private final Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>(0);
	private SQLWarning rootWarning = null;

	public PoiSSFConnection(URL file) throws IOException, InvalidFormatException {
		this(file, null);
	}
	public PoiSSFConnection(URL file, Properties info) throws IOException, InvalidFormatException {
		
		this.workbookURL = file;
		
		InputStream is = null; 
		try {
			is = file.openStream();			
			this.excelWorkbook = WorkbookFactory.create(is);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e1) {
					// Do nothing
				}
			}
		}
		
		if (info != null) {
			this.resultExtendedMetadataEnabled = "true".equalsIgnoreCase(info.getProperty(PoiSSFDriver.CONNECTION_PROPERTY_RS_EXTENDED_METADATA));
		}
		
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement() throws SQLException {
		return this.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return this.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return this.prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
	   final Pattern p = Pattern.compile("(?<=FROM ).*(?=( |$))", Pattern.CASE_INSENSITIVE);
	   
	   final Matcher m = p.matcher(sql);
	   if (m.find()) {
		   return m.group();
	   } else {
		   throw new SQLException("FROM not found");
	   }

	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (autoCommit != getAutoCommit()) {
			throw new SQLFeatureNotSupportedException("Always " + getAutoCommit());
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return false;
	}

	@Override
	public void commit() throws SQLException {
		if (!isClosed()) {
			if (!isReadOnly()) {
				
				FileOutputStream fos = null;
				
				try {
					fos = new FileOutputStream(new File(this.workbookURL.toURI()));
					excelWorkbook.write(fos);
				} catch (Throwable e) {
					throw new SQLException("error during commit", e);
				} finally {
					if (fos != null) {
						try {
							fos.close();
						} catch (IOException e) {
							SQLWarning myWarning = new SQLWarning("FileOutputStream not closed after commitcall", e);
							
							if (this.rootWarning != null) {
								this.rootWarning.setNextWarning(myWarning);
							} else {
								this.rootWarning = myWarning;
							}
						}
					}
				}
				
			} else {
				throw new SQLException("Connection is read only");
			}			
		} else {
			throw new SQLException(PoiSSFDriver.CONNECTION_IS_CLOSED);
		}
	}

	@Override
	public void rollback() throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void close() throws SQLException {
		this.excelWorkbook = null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.excelWorkbook == null;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return this.readOnly;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		//ignore as per doc
	}

	@Override
	public String getCatalog() throws SQLException {
		
		try {
			final String dir = new File(this.workbookURL.toURI()).getParent();
			if (dir != null) {
				return dir;
			} else {
				return "";
			}
		} catch (URISyntaxException e) {
			throw new SQLException("Error getting name", e);
		}
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		if (level != getTransactionIsolation()) {
			throw new SQLFeatureNotSupportedException();			
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		if (!isClosed()) {
			return this.rootWarning;
		} else {
			throw new SQLException(PoiSSFDriver.CONNECTION_IS_CLOSED);
		}
	}

	@Override
	public void clearWarnings() throws SQLException {
		this.rootWarning = null;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return this.createStatement(resultSetType, resultSetConcurrency, getHoldability());
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return this.prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return this.prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {	
		return this.typeMap;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		if (holdability != getHoldability()) {
			throw new SQLFeatureNotSupportedException();			
		}
	}

	@Override
	public int getHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {				
		int resultSetConcurrency2 = this.isReadOnly() ? ResultSet.CONCUR_READ_ONLY : resultSetConcurrency;
		return new PoiSSFStatement(this, resultSetType, resultSetConcurrency2, resultSetHoldability);			
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		
		int resultSetConcurrency2 = this.isReadOnly() ? ResultSet.CONCUR_READ_ONLY : resultSetConcurrency; 
		return new PreparedPoiSSFStatement(sql, this, resultSetType, resultSetConcurrency2, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLFeatureNotSupportedException("This method is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("This method is not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("This method is not supported");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return !isClosed();
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Do Nothing
		
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		// Do NOthing
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public Workbook getPoiWorkbook() {
		return this.excelWorkbook;
	}
	
	public String getSchema() {
		final String[] parts = this.workbookURL.getPath().split("/");
		return parts[parts.length-1];
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return 0;
	}
	
	/* package-private */ boolean isResultExtendedMetadataEnabled() {
		return resultExtendedMetadataEnabled;
	}

}
