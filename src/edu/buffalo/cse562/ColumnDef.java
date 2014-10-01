/**
 * 
 */
package edu.buffalo.cse562;

public class ColumnDef
{
	String tableName;
	String tableAlias;
	String columnName;
	String columnType;
	
	public ColumnDef() {}
	
	public ColumnDef(String cName, String cType, String tName, String tAlias)
	{	
		if(!"".equals(cName) && cName!=null)	
		columnName = cName.toUpperCase();
	if(!"".equals(cType) &&cType!=null)	
		columnType = cType.toUpperCase();
	if(!"".equals(tName) &&tName!=null)	
		tableName = tName.toUpperCase();
	if(	!"".equals(tAlias) && tAlias!=null)	
		tableAlias = tAlias.toUpperCase();
	}
	
	public ColumnDef(ColumnDef coldef)
	{		
		if(!"".equals(coldef.columnName) && coldef.columnName!=null) columnName = coldef.columnName.toUpperCase();
		if(!"".equals(coldef.columnType) && coldef.columnType!=null)	columnType = coldef.columnType.toUpperCase();
		if(!"".equals(coldef.tableName) && coldef.tableName!=null)	tableName = coldef.tableName.toUpperCase();
		if(!"".equals(coldef.tableAlias) && coldef.tableAlias!=null)	tableAlias = coldef.tableAlias.toUpperCase();
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getTableAlias() {
		return tableAlias;
	}
	
	public void setTableName(String tableName) {
		if(tableName!=null)	
		this.tableName = tableName.toUpperCase();
		else
			this.tableName = tableName;
	}
	
	public void setTableAlias(String tableAlias) {
		if(!"".equals(tableAlias) && tableAlias!=null)	
		this.tableAlias = tableAlias.toUpperCase();
		else
			this.tableAlias = tableAlias;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(String columnName) {
		if (!"".equals(columnName) && columnName!=null)
			this.columnName = columnName.toUpperCase();
		else
			this.columnName = columnName;
	}
	
	public String getColumnType() {
		return columnType;
	}
	
	public void setColumnType(String columnType) {
		if(!"".equals(columnType) && columnType!=null)
			this.columnType = columnType.toUpperCase();
		else
			this.columnType = columnType;
	}				 
}

