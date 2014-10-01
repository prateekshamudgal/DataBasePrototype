package edu.buffalo.cse562;

import java.io.IOException;
import java.util.List;

import edu.buffalo.cse562.Datum.Bool;
import edu.buffalo.cse562.Datum.Dt;
import edu.buffalo.cse562.Datum.Flt;
import edu.buffalo.cse562.Datum.Int;

import jdbm.*;

public class IndexSerializer implements Serializer<IndexRow>
{
	private static final long serialVersionUID = 1L;
	List<ColumnDef> colDef;
	
	public IndexSerializer(List<ColumnDef> colDef) {
		this.colDef = colDef;
	}

	@Override
	public IndexRow deserialize(SerializerInput in) throws java.io.IOException {
		
		//System.out.println("------Deserialize");
		Datum[] row = new Datum[colDef.size()];		
		for(int i = 0; i < colDef.size(); i++)
		{
			if ("INT".equalsIgnoreCase(colDef.get(i).getColumnType()))
				row[i] = new Datum.Int(in.readInt());
			else if ("DATE".equalsIgnoreCase(colDef.get(i).getColumnType()))
			{
				int a = in.readInt();
				int b = in.readInt();
				int c = in.readInt();
				//System.out.println(a+"-"+b+"-"+c);
				row[i] = new Datum.Dt(a, b, c);	
				//System.out.println("Dese DT: " + row[i]);
			}
			else if ("BOOL".equalsIgnoreCase(colDef.get(i).getColumnType()))
				row[i] = new Datum.Bool(in.readBoolean());
			else if ("FLOAT".equalsIgnoreCase(colDef.get(i).getColumnType())
					||"DECIMAL".equalsIgnoreCase(colDef.get(i).getColumnType()))
				row[i] = new Datum.Flt(in.readFloat());
			else //for strings
				row[i] = new Datum.Str(in.readUTF());
		}
		return new IndexRow(row);
	}
		
	@Override
	public void serialize(SerializerOutput out, IndexRow row) throws IOException
	{
		//System.out.println("------Serialize");
		for(int i = 0; i < colDef.size(); i++)
		{
			if ("INT".equalsIgnoreCase(colDef.get(i).getColumnType())) 
			{
				out.writeInt(((Int) row.data[i]).getValue());			
			}
			else if ("DATE".equalsIgnoreCase(colDef.get(i).getColumnType()))
			{
				out.writeInt(((Dt) row.data[i]).getYear());
				out.writeInt(((Dt) row.data[i]).getMonth());			
				out.writeInt(((Dt) row.data[i]).getDate());
			}
			else if ("BOOL".equalsIgnoreCase(colDef.get(i).getColumnType())) {
				out.writeBoolean(((Bool) row.data[i]).getValue());					
			}
			else if ("FLOAT".equalsIgnoreCase(colDef.get(i).getColumnType())
					||"DECIMAL".equalsIgnoreCase(colDef.get(i).getColumnType())) 
			{					
				out.writeFloat(((Flt) row.data[i]).getValue());					
			} 
			else 
			{
				out.writeUTF(row.data[i].toString());	
			}
		}
	}
}