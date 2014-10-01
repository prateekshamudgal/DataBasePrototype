package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.List;

public class TupleReadOperator implements Operator 
{
	HashMap<String, TableView> tv;
	TableView table;
	List<ColumnDef> columnDef;
	
	public TupleReadOperator(HashMap<String, TableView> tv, String tableName)
	{
		table = tv.get(tableName.toUpperCase());
		columnDef = table.getColumnDef();
		reset();
	}
		
	public Datum[] readOneTuple()
	{
		return table.readOneTuple();
	}
	
	@Override
	public void reset()
	{
		if(table!=null)
			table.reset();
	}

}
