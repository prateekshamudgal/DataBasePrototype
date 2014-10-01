package edu.buffalo.cse562;

import java.io.*;

public class IndexRow implements Serializable, Comparable<IndexRow> {

	private static final long serialVersionUID = 1L;
	public final Datum[] data;
	
	public IndexRow(Datum[] data) {
		this.data = data;
	}

	@Override
	public int compareTo(IndexRow other) {
		
		/*for(int i=0; i< data.length && i < other.data.length; i++)
		{
			int flag = data[i].compareTo(other.data[i]);
			if(flag != 0)
				return flag;
		}
		return 0;*/
		return data[0].compareTo(other.data[0]);
	}
	
	public int hashCode() 
	{
		return data.toString().toUpperCase().hashCode();
    }
	
	public String toString()
	{
		String row = "";
		for(int i=0; i<data.length-1; i++)
			row = row + data[i]+"|";
		row = row + data[data.length-1];
		return row;
	}
}
