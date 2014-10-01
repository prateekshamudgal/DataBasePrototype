package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class TableView implements Operator
{	
	ArrayList<Datum[]> list;
	List<ColumnDef> coldef;
	int length;
	int counter;
	
	public TableView()
	{
		list = new ArrayList<Datum[]>();
		coldef = new ArrayList<ColumnDef>();
		length = 0;
		counter = 0;
	}
	
	public void addTuple(Datum[] tuple)
	{
		list.add(tuple);
		length++;
	}
	
	@Override
	public Datum[] readOneTuple() {
		if(counter < length)
		{
			Datum[] d = list.get(counter);
			counter++;
			return d;
		}
		return null;
	}
	
	/*public Datum getColumnValue(String colName) // get column value from current tuple
	{
		for(int i=0; i< coldef.size(); i++)
		{
			if(coldef.get(i).getColumnName().equalsIgnoreCase(colName))
			{
				return list.get(counter)[i];
			}
		}
		return null;
	}*/
	
	public Datum getColumnValue(String colName, Datum[] d) // get column value from passed tuple
	{
		for(int i=0; i< coldef.size(); i++)
		{
			if(coldef.get(i).getColumnName().equalsIgnoreCase(colName))
			{
				return d[i];
			}
		}
		return null;
	}
	
	public Datum getColumnValue(String tableName, String colName, Datum[] d) // get column value from passed tuple
	{
		if(tableName.startsWith("TEMP_"))
			tableName = tableName.replace("TEMP_", "");
		for(int i=0; i< coldef.size(); i++)
		{
			if(coldef.get(i).getColumnName().equalsIgnoreCase(colName) && coldef.get(i).getTableName().equalsIgnoreCase(tableName))
			{
				return d[i];
			}
		}
		return null;
	}
	
	public int getColmnIndex(String name)
	{
		for(int i=0;i<coldef.size();i++)
		{
			if(coldef.get(i).getColumnName().equalsIgnoreCase(name))
				return i;
			
		}
		return -1;
	}
	
	public Datum getColumnValue(String colName, int row) // get column value from given row/tuple
	{
		String tableName = "";
		if(colName.contains("."))
		{
			tableName = colName.split("\\.")[0];
			colName = colName.split("\\.")[1];
		}
			
		for(int i=0; i< coldef.size(); i++)
		{
			if("".equals(tableName))
			{
				if(coldef.get(i).getColumnName().equalsIgnoreCase(colName))
				{
					return list.get(row)[i];
				}
			}
			else
			{
				if(coldef.get(i).getColumnName().equalsIgnoreCase(colName) && coldef.get(i).getTableName().equalsIgnoreCase(tableName))
				{
					return list.get(row)[i];
				}
			}
		}
		return null;
	}
	
	public void removeTuple()
	{
		if(counter < length)
		{
			list.remove(counter);
			length--;
		}
	}
	
	public void removeTuple(int row)
	{
		if(row < length)
		{
			list.remove(row);
			length--;
		}
	}
	
	@Override
	public void reset() {
		counter = 0;
	}
	
	public List<ColumnDef> getColumnDef()
	{
		return coldef;
	}	

	public void setColDef(List<ColumnDefinition> inputDef, String name, String alias) 
	{		
		for(int i =0 ;i < inputDef.size() ;i++)
		 {	
			ColumnDef colDefObj = new ColumnDef(inputDef.get(i).getColumnName(),
					inputDef.get(i).getColDataType().getDataType(),name, alias);	
			coldef.add(colDefObj);
		}
	}
	
	public void setColDef(List<ColumnDef> inputDef) 
	{		
		coldef = new ArrayList<ColumnDef>();
		for(int i =0 ;i < inputDef.size() ;i++)
		 {	
			ColumnDef colDefObj = new ColumnDef(inputDef.get(i));	
			coldef.add(colDefObj);
		}
	}
	
	public void appendColDef(List<ColumnDef> inputDef) 
	{		
		for(int i =0 ;i < inputDef.size() ;i++)
		 {	
			ColumnDef colDefObj = new ColumnDef(inputDef.get(i));	
			coldef.add(colDefObj);
		}
	}
	
	public int getLength()
	{
		return length;
	}	
}

