package de.einwesen.jdbc.poissf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import de.einwesen.jdbc.IndexBasedResultSet;

public class PoiSSFResultSet extends IndexBasedResultSet implements ResultSet {

	private PoiSSFStatement parentStatement = null;	
	private Sheet sheet = null;
	private Row currentRow = null; 
	private FormulaEvaluator evaluator = null; 
	
	private int rowCount = 0;	
	private int currentRowIndex = 0;
	
	private boolean closed = false;
	private boolean wasNull = false;
	private boolean wasDeleted = false;

	private static Class<?> CASTABLE_CLASSES[] = {Date.class, Timestamp.class, Time.class};
	
	private PoiSSFResultSetMetaData metaData = null;
	
	public PoiSSFResultSet(String sql, PoiSSFStatement parentStatement) throws SQLException {
		this.parentStatement = parentStatement;
		this.sheet = parentStatement.getConnection().getPoiWorkbook().getSheet(parentStatement.getConnection().nativeSQL(sql));
		
		this.evaluator = parentStatement.getConnection().getPoiWorkbook().getCreationHelper().createFormulaEvaluator();
		
		// Owing to idiosyncrasies in the excel file format, if the result of calling this method is zero, you can't tell if that means there are zero rows on the sheet, or one at position zero. For that case, additionally call getPhysicalNumberOfRows() to tell if there is a row at position zero or not.
		this.rowCount = sheet.getLastRowNum() + 1;
		if (this.rowCount == 1) {
			if (this.sheet.getPhysicalNumberOfRows() == 0) {
				this.rowCount = 0;
			}
		}
	}
	
	private void checkOpen() throws SQLException {

		if (this.parentStatement.getConnection().isClosed()) {
			throw new SQLException(PoiSSFDriver.CONNECTION_IS_CLOSED);
		}
		
		if (this.parentStatement.isClosed()) {
			throw new SQLException(PoiSSFDriver.STATEMENT_IS_CLOSED);			
		}
		
		if (this.closed) {
			throw new SQLException(PoiSSFDriver.RESULTSET_IS_CLOSED);
		}
	}
	
	private void checkValidRow() throws SQLException {
		if (this.currentRow == null) {
			throw new SQLException(PoiSSFDriver.CURSOR_ON_INVALID_ROW);
		}		
	}
	
	private Cell getCell(int sqlIndex) throws SQLException {
		checkValidRow();
		checkOpen();	
		
		if (sqlIndex > 0 && sqlIndex <= this.getMetaData().getColumnCount()) {
			return this.currentRow.getCell(sqlIndex-1);
		} else {
			throw new SQLException(String.format(PoiSSFDriver.INVALID_COLUMN_INDEX, sqlIndex));
		}
	}

//	private Cell getCellForUpdate(int sqlIndex) throws SQLException {
//		if (this.getConcurrency() == ResultSet.CONCUR_UPDATABLE) {
//			//return getCell(sqlIndex);
//			throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);
//		} else {
//			throw new SQLException(PoiSSFDriver.RESULT_SET_NOT_UPDATETABLE);
//		}
//	}
	
	private BigDecimal getBigDecimal(int columnIndex, boolean nullIsZero) throws SQLException {
		
		final Cell cell = getCell(columnIndex);
		
		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}
		
		switch (cellType) {
		        case Cell.CELL_TYPE_NUMERIC:
		            return BigDecimal.valueOf(cell.getNumericCellValue());
		        		        
		        case Cell.CELL_TYPE_BLANK:
		        	this.wasNull = true;
		        	if (nullIsZero) {
		        		return BigDecimal.valueOf(0);
		        	} else {
		        		return null;
		        	}
		        	
		        case Cell.CELL_TYPE_STRING:
		        case Cell.CELL_TYPE_BOOLEAN:
		        	throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);
		        	
		        case Cell.CELL_TYPE_ERROR:
		        case Cell.CELL_TYPE_FORMULA:  // CELL_TYPE_FORMULA will never occur
		        default:
		            throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}		
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {

		final Cell cell = getCell(columnIndex);
		
		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}
		

		
		switch (cellType) {
		        case Cell.CELL_TYPE_BOOLEAN:
		            return new Boolean(cell.getBooleanCellValue());
		        case Cell.CELL_TYPE_NUMERIC:
		        	if (HSSFDateUtil.isCellDateFormatted(cell)) {
		        		return HSSFDateUtil.getJavaDate(cell.getNumericCellValue() /* , timezone */);
		        	} else {
		        		return new BigDecimal(String.valueOf(cell.getNumericCellValue()));		        		
		        	}
		        case Cell.CELL_TYPE_STRING:
		            return cell.getStringCellValue();
		        case Cell.CELL_TYPE_BLANK:
		        	this.wasNull = true;
		            return null;
		        case Cell.CELL_TYPE_ERROR:
		        case Cell.CELL_TYPE_FORMULA:  // CELL_TYPE_FORMULA will never occur
		        default:
		            throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}

	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {

		final Cell cell = getCell(columnIndex);
		
		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}
		
		switch (cellType) {
		        case Cell.CELL_TYPE_BOOLEAN:
		            return cell.getBooleanCellValue();
		        case Cell.CELL_TYPE_NUMERIC:
		        	return cell.getNumericCellValue() > 0;
		        case Cell.CELL_TYPE_BLANK:
		        	this.wasNull = true;
		            return false;		            
		        case Cell.CELL_TYPE_STRING:
		        	final String tmp = cell.getStringCellValue();
		        	if ("1".equals(tmp)) {
		        		return true;
		        	} else if ("0".equals(tmp)){
		        		return false;
		        	} else {
		        		throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);		        		
		        	}
		        case Cell.CELL_TYPE_ERROR:
		        case Cell.CELL_TYPE_FORMULA:  // CELL_TYPE_FORMULA will never occur
		        default:
		            throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}		

	}
	
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		final Cell cell = getCell(columnIndex);
		
		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}		
		
		switch (cellType) {
			case Cell.CELL_TYPE_ERROR:
				return cell.getErrorCellValue();
			case Cell.CELL_TYPE_BLANK:
				this.wasNull = true;
				return 0;		            
			case Cell.CELL_TYPE_STRING:
				return (byte)cell.getStringCellValue().charAt(0);
			case Cell.CELL_TYPE_BOOLEAN:			
	        case Cell.CELL_TYPE_NUMERIC:
        		throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);		        		
	        case Cell.CELL_TYPE_FORMULA:  // CELL_TYPE_FORMULA will never occur
	        default:
	            throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}
	}	
	
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		final Cell cell = getCell(columnIndex);
		
		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}		
		
		switch (cellType) {
			case Cell.CELL_TYPE_ERROR:
				return new byte[]{cell.getErrorCellValue()};
			case Cell.CELL_TYPE_BLANK:
				this.wasNull = true;
				return null;		            
			case Cell.CELL_TYPE_STRING:
				return cell.getStringCellValue().getBytes();
			case Cell.CELL_TYPE_BOOLEAN:			
	        case Cell.CELL_TYPE_NUMERIC:
        		throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);		        		
	        case Cell.CELL_TYPE_FORMULA:  // CELL_TYPE_FORMULA will never occur
	        default:
	            throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}
	}
	
	private void updateJavaDate(int columnIndex, java.util.Date x, int sqlType) throws SQLException {
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
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
	public boolean next() throws SQLException {
		return this.relative(1);
	}

	@Override
	public void close() throws SQLException {
		this.closed = true;		
	}

	@Override
	public boolean wasNull() throws SQLException {
		return this.wasNull;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		
		final Object o = getObject(columnIndex);
		if (this.wasNull) {
			return "null";
		} else {
			return o.toString();
		}
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, true).shortValue();
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, true).intValue();
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, true).longValue();
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, true).floatValue();
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, true).doubleValue();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return getDate(columnIndex, null);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return getTime(columnIndex, null);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return getTimestamp(columnIndex, null);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return getBinaryStream(columnIndex);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		final byte[] buf = getBytes(columnIndex); 
		return !this.wasNull ? new ByteArrayInputStream(buf) : null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// Do nothing
		
	}

	@Override
	public String getCursorName() throws SQLException {
		return this.getClass().getSimpleName().concat(String.valueOf(this.hashCode()));
	}

	@Override
	public PoiSSFResultSetMetaData getMetaData() throws SQLException {
		if (metaData == null) {
			metaData = PoiSSFResultSetMetaData.getInstance(this);
		}
		return metaData;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return this.getMetaData().getColumnIndex(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		final Object o = getObject(columnIndex); 
		if (!this.wasNull) {
			return new StringReader(o.toString());
		} else {
			return null;
		}
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return getBigDecimal(columnIndex, false);		
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return this.currentRowIndex < 1;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return this.currentRowIndex > rowCount;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return this.currentRowIndex == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		return this.currentRowIndex == this.rowCount;
	}

	@Override
	public void beforeFirst() throws SQLException {
		this.absolute(0);
	}

	@Override
	public void afterLast() throws SQLException {
		if (this.rowCount > 0) {
			this.absolute(this.rowCount + 1);					
		}
	}

	@Override
	public boolean first() throws SQLException {
		return this.absolute(1);
	}

	@Override
	public boolean last() throws SQLException {
		return this.absolute(this.rowCount);
	}

	@Override
	public int getRow() throws SQLException {
		if  (this.currentRow != null && !this.isBeforeFirst() && !this.isAfterLast()) {
			return this.currentRowIndex;			
		} else {
			return 0;
		}
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		checkOpen();
		
		if (this.wasDeleted) {
			this.currentRowIndex--;
			this.wasDeleted = false;
		}
		
		int newRow = 0;
		
		if (row > 0) {
			
			newRow = row;
			
		} else if (row < 0) {
			
			newRow = this.rowCount + row + 1; 
			
		} else {
			throw new SQLException("Illegal row");
		}
		
		if (row >= this.currentRowIndex || this.parentStatement.getResultSetType() != ResultSet.TYPE_FORWARD_ONLY) {
			this.currentRowIndex = newRow;
		} else {
			throw new SQLException("FORWARD_ONLY");
		}
		
		if (this.isBeforeFirst()) {
			this.currentRowIndex = 0;
			this.currentRow = null;
			return false;
		} else if (this.isAfterLast()) {
			this.currentRowIndex = this.rowCount + 1;
			this.currentRow = null;
			return false;
		} else {
			this.currentRow = this.sheet.getRow(this.currentRowIndex-1);
			return true;
		}
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return this.absolute(this.currentRowIndex + rows);
	}

	@Override
	public boolean previous() throws SQLException {
		return this.relative(-1);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException();
		
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return this.parentStatement.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public int getFetchSize() throws SQLException {
		return this.parentStatement.getFetchSize();
	}

	@Override
	public int getType() throws SQLException {
		return this.parentStatement.getResultSetType(); 
	}

	@Override
	public int getConcurrency() throws SQLException {
		return this.parentStatement.getResultSetConcurrency();
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		// TODO implement for real
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {		
		// TODO implement for real
		return false; //throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return this.wasDeleted;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		// Convert to double
		updateDouble(columnIndex, x);		
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		// convert to double
		updateDouble(columnIndex, x);				
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		if (x != null) {
			updateDouble(columnIndex, x.doubleValue());
		} else {
			updateNull(columnIndex);
		}
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		if (x != null) {
			// TODO implement for real		
			throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
		} else {
			updateNull(columnIndex);
		}
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		if (x != null) {
			updateJavaDate(columnIndex, x, Types.DATE);
		} else {
			updateNull(columnIndex);
		}		
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		if (x != null) {
			updateJavaDate(columnIndex, x, Types.TIME);
		} else {
			updateNull(columnIndex);
		}		
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		if (x != null) {
			updateJavaDate(columnIndex, x, Types.TIMESTAMP);
		} else {
			updateNull(columnIndex);
		}		
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		updateObject(columnIndex, x);		
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		if (x != null) {
			
			if (String.class.equals(x.getClass())) {
				updateString(columnIndex, x.toString());
			} else if (java.util.Date.class.isAssignableFrom(x.getClass())) {
				updateJavaDate(columnIndex, (java.util.Date)x, Types.OTHER);
			} else if (Number.class.isAssignableFrom(x.getClass())){
				updateDouble(columnIndex, ((Number)x).doubleValue());
			} else {
				throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);
			}			
		} else {
			updateNull(columnIndex);
		}		
	}

	@Override
	public void insertRow() throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);				
	}

	@Override
	public void updateRow() throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void deleteRow() throws SQLException {
		checkValidRow();
		this.sheet.removeRow(this.currentRow);
		this.currentRow = null;
		this.rowCount--;		
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		// TODO implement for real		
		throw new SQLFeatureNotSupportedException(PoiSSFDriver.NOT_IMPLEMENTED_YET);		
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		this.absolute(currentRowIndex);		
	}

	@Override
	public PoiSSFStatement getStatement() throws SQLException {
		return this.parentStatement;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		this.parentStatement.getConnection().getTypeMap();		
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {

		try {
			return (Date)getObject(columnIndex);
		} catch (ClassCastException e) {
			throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE);
		}		
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return (Time)getObject(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {		
		return (Timestamp)getObject(columnIndex);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public int getHoldability() throws SQLException {
		return this.parentStatement.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		this.updateString(columnIndex, nString);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return getCharacterStream(columnIndex);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		updateCharacterStream(columnIndex, x, length);		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		updateClob(columnIndex, reader, length);		
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		updateCharacterStream(columnIndex, x);	
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		updateAsciiStream(columnIndex, x, -1);	
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		updateBinaryStream(columnIndex, x, -1);		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		updateCharacterStream(columnIndex, x, -1);		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		updateBlob(columnIndex, inputStream, -1);		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		updateClob(columnIndex, reader, -1);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		updateClob(columnIndex, reader);		
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type != null) {
			Object o = null;
			if (String.class.equals(type)) {
				o = getString(columnIndex);
			} else if (tryConvertByCast(type)) {
				o = getObject(columnIndex);
			} else if (Number.class.isAssignableFrom(type)) {
				final BigDecimal bd = getBigDecimal(columnIndex);
				if (bd != null) {
					if (BigDecimal.class.equals(type)) {
						o = bd;
					} else if (Integer.class.equals(type)) {
						o = Integer.valueOf(bd.intValue());
					} else if (Double.class.equals(type)) {
						o = Integer.valueOf(bd.intValue());
					} else if (Long.class.equals(type)) {
						o = Long.valueOf(bd.longValue());
					} else if (Float.class.equals(type)) {
						o = Float.valueOf(bd.toString());
					} else if (BigInteger.class.equals(type)) {
						o = bd.toBigInteger();
					} else {
						throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE, new IllegalArgumentException(type.getClass().getName()));
					}
				}				
			} else if (Boolean.class.equals(type)) {
				final boolean b = getBoolean(columnIndex);
				if (!this.wasNull()) {
					o = Boolean.valueOf(b);
				}
			} else {
				throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE, new IllegalArgumentException(type.getClass().getName()));
			}
			
			try {
				return (T)o;
			} catch (ClassCastException e) {
				throw new SQLException(PoiSSFDriver.INCOMPATIBLE_DATATYPE, e);
			}
			
		} else {
			throw new SQLException(String.format(PoiSSFDriver.PARAMETER_MAY_NOT_BE_NULL, "type"));
		}
	}
	
	
	private static boolean tryConvertByCast(Class<?> type) {
		
		for (Class<?> c: CASTABLE_CLASSES) {
			if (c.equals(type)) {
				return true;
			}
		}
		
		return false;
	}	
	
	/* Package-private */ JDBCType getCellJDBCTypeAtCurrentRow(int columnIndex) throws SQLException {
		checkOpen();
		checkValidRow();

		final Cell cell = getCell(columnIndex);

		int cellType = cell.getCellType();
		if (cellType == Cell.CELL_TYPE_FORMULA) {
			cellType = evaluator.evaluateInCell(cell).getCellType();
		}

		switch (cellType) {
		case Cell.CELL_TYPE_BOOLEAN:
			return JDBCType.BOOLEAN;
		case Cell.CELL_TYPE_NUMERIC:
			if (HSSFDateUtil.isCellDateFormatted(cell)) {
				return JDBCType.TIMESTAMP;
			} else {
				return JDBCType.NUMERIC;
			}
		case Cell.CELL_TYPE_STRING:
			return JDBCType.VARCHAR;

		case Cell.CELL_TYPE_BLANK:
			return JDBCType.NULL;

		case Cell.CELL_TYPE_ERROR:
		case Cell.CELL_TYPE_FORMULA: // CELL_TYPE_FORMULA will never occur
			return JDBCType.DATALINK;
		default:

			throw new SQLException(PoiSSFDriver.ERROR_RETRIEVING_DATA);
		}
	}
	
	/* Package-private */ Sheet getPoiSheet() {
		return this.sheet;
	}

}
