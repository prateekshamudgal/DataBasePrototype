package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class BlockScanNode extends PlanNode.Leaf  implements Operator
{
	public final String table;
	File f;
	BufferedReader br;
	List<ColumnDef> columnDef;
	String line = null;
	String nextLine = null;
	int tupleSize = 0;	
	
	public BlockScanNode(File f, List<ColumnDef> colDef)
	{
		this.f = f;
		this.columnDef = colDef;
		this.table = colDef.get(0).tableName;
		reset();
	}
	
	@Override
    public List<ColumnDef> getSchemaVars() {
		return this.columnDef;
    }
	
	@Override
	public Datum[] readOneTuple()
	{					
		if(br == null) return null;
		
		seeNext(); 
		if(line == null) 
			return null;	

		return getDatumValues(line.split("\\|"));					
	}
	
	private Datum[] getDatumValues(String[] cols)
	{
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
	
	@Override
	public void reset() // read the main file again
	{
		close();
		try
		{
			br = new BufferedReader(new FileReader(f));
			line = null;
			nextLine = br.readLine();			
		}
		catch(IOException e) 
		{
			e.printStackTrace();
			br = null;
		}		
	}
	
	public void close() // close the main file reader
	{
		try 
		{
			if(br!=null)
				br.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			br = null;
		}
	}
	
	public String seeNext() // used for external sort, nextLine from peek()
	{
		try 
		{			
			line = nextLine;
			if(line != null)
				tupleSize = line.length();
			else
				tupleSize = 0;
			
			if(nextLine != null)			
				nextLine = br.readLine();				
			return nextLine;
			
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		return null;		
	}
	
	public int getTupleSize()
	{
		return tupleSize;
	}
	
	public Datum[] peek()
	{		
		if(nextLine != null)
			return getDatumValues(nextLine.split("\\|"));
		else
			return null;			
	}
}
