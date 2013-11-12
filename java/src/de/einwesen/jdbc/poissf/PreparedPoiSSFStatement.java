package de.einwesen.jdbc.poissf;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;

public class PreparedPoiSSFStatement extends PoiSSFStatement implements PreparedStatement {

	private String sqlString = null;
	private HashMap<Integer, Object> parameter = new HashMap<Integer, Object>();
	
	public PreparedPoiSSFStatement(String sql, PoiSSFConnection connection, int type, int concur, int hold) throws SQLException {
		super(connection, type, concur, hold);
		this.sqlString = sql;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		return this.executeQuery(this.sqlString);
	}

	@Override
	public int executeUpdate() throws SQLException {
		return this.executeUpdate(this.sqlString);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		this.parameter.put(parameterIndex, null);
		
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void clearParameters() throws SQLException {
		this.parameter.clear();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public boolean execute() throws SQLException {
		return this.execute(this.sqlString);
	}

	@Override
	public void addBatch() throws SQLException {
		this.addBatch(this.sqlString);		
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		this.parameter.put(parameterIndex, x);	
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		this.parameter.put(parameterIndex, x);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		this.parameter.put(parameterIndex, null);		
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		this.parameter.put(parameterIndex, value);		
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		this.parameter.put(parameterIndex, value);				
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		this.parameter.put(parameterIndex, xmlObject);
		
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		this.parameter.put(parameterIndex, x);		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

}
