package de.einwesen.jdbc.poissf;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFSheet;

import de.einwesen.jdbc.UnknownException;

public class PoiSSFResultSetMetaData implements ResultSetMetaData {

	
	private int columnCount = -1;
	private String catalog = "";
	private String schema = "";
	private String tableName = "";
	private boolean readOnly = true;
	
	private List<String> columnNames = null;
		
	private PoiSSFResultSet parentResultSet = null;

	/* Package-Private */ static PoiSSFResultSetMetaData getInstance(PoiSSFResultSet rs) throws SQLException {
		final Sheet sheet = rs.getPoiSheet();
		if (sheet instanceof XSSFSheet) {
			return new PoiSSFResultSetMetaData((XSSFSheet)sheet, rs);
		} else if (sheet instanceof HSSFSheet) {
			return new PoiSSFResultSetMetaData((HSSFSheet)sheet, rs);
		} else {
			return null;
		}
	}
		
	private PoiSSFResultSetMetaData(XSSFSheet sheet, PoiSSFResultSet rs) throws SQLException {
			
		final String[] sheetDimensions = sheet.getCTWorksheet().getDimension().getRef().split(":");

		final String rightColName = sheetDimensions[1].replaceAll("\\d", "");
				
		this.columnCount = CellReference.convertColStringToIndex(rightColName) + 1;
		this.catalog = rs.getStatement().getConnection().getCatalog();
		this.schema = rs.getStatement().getConnection().getSchema();
		this.tableName = sheet.getSheetName();
		this.readOnly = rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY;
		
		this.columnNames = new ArrayList<String>(columnCount);
		
		for (int i=0; i< columnCount; i++) {
			columnNames.add(CellReference.convertNumToColString(i));
		}
		
		if (rs.getStatement().getConnection().isResultExtendedMetadataEnabled()) {
			this.parentResultSet = rs;
		}
		
	}
	
	private PoiSSFResultSetMetaData(HSSFSheet sheet, PoiSSFResultSet rs) throws SQLException {
		this.catalog = rs.getStatement().getConnection().getCatalog();
		this.schema = rs.getStatement().getConnection().getSchema();
		this.tableName = sheet.getSheetName();
		this.readOnly = rs.getConcurrency() == ResultSet.CONCUR_READ_ONLY;
		this.columnCount = getNumberOfColumns(sheet);

		this.columnNames = new ArrayList<String>(columnCount);
		
		for (int i=0; i< columnCount; i++) {
			columnNames.add(CellReference.convertNumToColString(i));
		}
		
		if (this.columnCount == 0) {
			throw new SQLException("Can't figure out number of columns");
		}
	}
	
	private int getNumberOfColumns(HSSFSheet sheet) {

		// getting number of cols, damn inefficient...
		int rowCount = sheet.getLastRowNum() + 1;
		if (rowCount == 1) {
			if (sheet.getPhysicalNumberOfRows() == 0) {
				rowCount = 0;
			}
		}
				
		short columnCount = 0;
		for (int r=0; r < rowCount; r++) {
			short cellCount = sheet.getRow(r).getLastCellNum();
			
			if (cellCount > columnCount) {
				columnCount = cellCount;
			}
		}
		
		return columnCount;
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
	public int getColumnCount() throws SQLException {
		return this.columnCount;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		/// ?????
		throw new UnknownException();
		
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// Datatype of column is unknown
		throw new UnknownException();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return ResultSetMetaData.columnNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// Datatype of column is unknown
		throw new UnknownException();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumnName(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return columnNames.get(column - 1);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return this.schema;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		// Datatype of column is unknown
		throw new UnknownException();
	}

	@Override
	public int getScale(int column) throws SQLException {
		// Datatype of column is unknown
		throw new UnknownException();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return this.tableName;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return this.catalog;
	}

	public JDBCType getColumnJDBCType(int column) throws SQLException {
		if (this.parentResultSet != null) {
			return this.parentResultSet.getCellJDBCTypeAtCurrentRow(column);
		} else {
			return JDBCType.OTHER;
		}
	}
	@Override
	public int getColumnType(int column) throws SQLException {		
		return getColumnJDBCType(column).getVendorTypeNumber().intValue();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumnJDBCType(column).getName();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return readOnly;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return !this.isReadOnly(column);
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return this.isWritable(column);
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		
		switch (getColumnJDBCType(column)) {
			case BOOLEAN:
				return Boolean.class.getName();
			case TIMESTAMP:
				return java.util.Date.class.getName();
			case NUMERIC:
				return BigDecimal.class.getName();
			case VARCHAR:
				return String.class.getName();
			case NULL:				
			case DATALINK:
			case OTHER:
			default:
				return Object.class.getName();
		}
	}
	
	public int getColumnIndex(String label) throws SQLException {
		int idx =  columnNames.indexOf(label);
		if (idx != -1) {
			return idx;
		} else {
			throw new SQLException("invalid column label '" + label+ "'");
		}
	}

}
