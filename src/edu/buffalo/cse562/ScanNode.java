package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;


public class ScanNode extends PlanNode.Leaf implements Operator {
  public final String table;
  public final List<ColumnDef> schema;
  BufferedReader input;
  List<ColumnDef> columnDef;
  File f;	
  
  public ScanNode(File f, List<ColumnDef> colDef)
  {
	  this.schema = colDef;
	  this.table = colDef.get(0).tableName;
	  this.f = f;
	  this.columnDef = colDef;
	  reset();
  }
  
  public List<ColumnDef> getSchemaVars() {
	  return this.columnDef;
  }
  
  public Datum[] readOneTuple()
	{
		if(input == null) {return null;}
		String line = null;
		try {
			line = input.readLine();
		} 
		catch(IOException e){
			e.printStackTrace();
		}
		if(line == null) {return null;}
		
		String[] cols = line.split("\\|");
		//Get column type
		Datum[] ret = new Datum[cols.length];
		for(int i = 0; i < cols.length; i++)
		{
			if ("INT".equalsIgnoreCase(columnDef.get(i).getColumnType()))
				ret[i] = new Datum.Int(cols[i]);
			else if ("DATE".equalsIgnoreCase(columnDef.get(i).getColumnType()))
				ret[i] = new Datum.Dt(cols[i]);
			else if ("FLOAT".equalsIgnoreCase(columnDef.get(i).getColumnType())
					||"DECIMAL".equalsIgnoreCase(columnDef.get(i).getColumnType()))
				ret[i] = new Datum.Flt(cols[i]);
			else
				ret[i] = new Datum.Str(cols[i]);
		}
		return ret;		
	}
  
  public void reset()
	{
		try
		{
			input = new BufferedReader(new FileReader(f));
		}
		catch(IOException e) 
		{
			e.printStackTrace();
			input = null;
		}		
	}
}
