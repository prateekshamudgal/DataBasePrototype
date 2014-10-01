package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.PriorityQueue;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortNode extends PlanNode.Unary implements Operator {
	List<?> orderByElements;	
	List<ColumnDef> schema;
	Comparator<Datum[]> comp = null;
	
	TableView resultTable = new TableView(); // For storing in memory when swapDir = false
	BlockScanNode sortedFileScanner = null; // For storing on disk when swapDir = true
	File swapDir = null;	
	
	public SortNode(List<?> orderByElements, File swapDirectory)
	{ 
		this.orderByElements = orderByElements; 
		swapDir = swapDirectory;
	}
		
	public static SortNode make(PlanNode source, List<?> orderByElements, File swapDir)
    {
		//orderByElements should be the same with the one in pselect;
		//You can also make orderByElements to be List<ColumnDef>;
		SortNode sn = new SortNode(orderByElements, swapDir);
		sn.setChild(source);
		sn.schema = sn.getSchemaVars();
		sn.orderBy();
		sn.sort();
		return sn;		
    }
	
	public List<ColumnDef> getSchemaVars() {
		return getChild().getSchemaVars();
	}
	
	public void sort()
	{		
		PlanNode source = getChild();
		if(swapDir != null) // sort externally
		{			
			List<File> tempFiles = sortIndividualBlocks(source, comp);
			File f = mergeSort(tempFiles);
			sortedFileScanner = new BlockScanNode(f, schema);
		}
		else // sort in memory
		{
			Datum[] row =  source.readOneTuple();
			resultTable.setColDef(source.getSchemaVars());
			while(row != null)
			{
				resultTable.addTuple(row);
				row =  source.readOneTuple();
			}
			Collections.sort(resultTable.list, comp);
		}
	}
	
	// Defines the custom comparator
	public void orderBy()
	{
		final int[] idx_order = new int[orderByElements.size()];
		final boolean[] isAsc = new boolean[orderByElements.size()];
		int idx = 0;
		for (int i = 0; i < orderByElements.size(); i++)
		{
			isAsc[i] = true;
			for (int j = 0; j < this.schema.size(); j++)
			{
				Column colName;
				if (orderByElements.get(i) instanceof OrderByElement)
				     colName = (Column) ((OrderByElement) orderByElements.get(i)).getExpression();
				else 
					colName = (Column)orderByElements.get(i);
				//Column colName = ((Column) orderByElements.get(i));
				
				String tabName = colName.getTable().getName(); // OrderBy TableName
				String colmName = colName.getColumnName(); // OrderBy ColumnName
				ColumnDef cDef = this.schema.get(j);
				
				if(tabName==null || "".equals(tabName))
				{
					if (colmName.equalsIgnoreCase(cDef.columnName))
					{
						idx_order[idx] = j;
						if (orderByElements.get(i) instanceof OrderByElement)
							isAsc[idx ++] = ((OrderByElement) orderByElements.get(i)).isAsc();
					}	
				}
				else
				{						
					if (colmName.equalsIgnoreCase(cDef.columnName) && tabName.equalsIgnoreCase(cDef.tableName))
					{
						idx_order[idx] = j;
						if (orderByElements.get(i) instanceof OrderByElement)
							isAsc[idx ++] = ((OrderByElement) orderByElements.get(i)).isAsc();
					}					
				}
			}
		}
		
		comp = new Comparator<Datum[]>() 
		{
			public int compare(Datum[] t1, Datum[] t2) 
			{
				int cnt_order = 0;
				int result = 0;
				while (cnt_order < idx_order.length)
				{
					if (t1[idx_order[cnt_order]].compareTo(t2[idx_order[cnt_order]]) != 0)
					{
						if (isAsc[cnt_order])
							result = t1[idx_order[cnt_order]].compareTo(t2[idx_order[cnt_order]]);
						else
							result = t2[idx_order[cnt_order]].compareTo(t1[idx_order[cnt_order]]);
						break;
					}
					cnt_order ++;
				}
				return result;
			}
		};
	}

	@Override
	public Datum[] readOneTuple() {
		// After we get sorted result, if we store it in TableView, then return one tuple of the final result.
		Datum[] ret = null;
		if(swapDir != null) // sort externally
		{			
			ret = sortedFileScanner.readOneTuple();			 
		}
		else // sort in memory
		{
			ret = resultTable.readOneTuple();
		}
		return ret;
	}

	@Override
	public void reset() {
		getChild().reset();
	}
	
	private List<File> sortIndividualBlocks(PlanNode source, Comparator<Datum[]> comp)
	{
		PlanNode oper = source;
		long  maxBlockSize = Runtime.getRuntime().freeMemory()/8;
		long currentSize = 0;
		List<File> fileList = new ArrayList<File>();
		
		Datum[] row = oper.readOneTuple();
		List<Datum[]> tupleList = new ArrayList<Datum[]>();
		while(row != null)
		{
			currentSize += oper.getTupleSize();
			if(currentSize >= maxBlockSize) 
			{
				if(tupleList.size() > 0)
				{
					Collections.sort(tupleList, comp); // sort tuple-list
					fileList.add(addToFile(tupleList)); // save in temporary file
					tupleList.clear();
					currentSize = 0;
				}								
			}
			tupleList.add(row);	// keep adding to tupleList		
			row = oper.readOneTuple();
		}
		if(tupleList.size() > 0) // write last page
		{
			Collections.sort(tupleList, comp); // sort tuple-list
			fileList.add(addToFile(tupleList)); // save in temporary file
		}
		return fileList;
	}
	
	private File addToFile(List<Datum[]> list)
	{
		File f = null;
		try
		{
			f = File.createTempFile("ExtSortPass1-", "", swapDir);
			f.deleteOnExit();
			
			BufferedWriter tempbw = new BufferedWriter(new FileWriter(f));
			for(Datum[] row : list)
			{
				String line = DatumToString(row);
				tempbw.write(line);
				tempbw.newLine();
			}
			tempbw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		return f;
	}
	
	private String DatumToString(Datum[] row)
	{
		String line = "";
		for(int i=0; i<row.length-1; i++)
			line += row[i].toString() + "|";
		line += row[row.length-1].toString();
		return line;
	}
	
	private File mergeSort(List<File> files) 
	{
		if(files.size() == 1) // no need to merge
		{			
			return files.get(0);
		}	

		PriorityQueue<BlockScanNode> queue = new PriorityQueue<BlockScanNode>(5, 
			            new Comparator<BlockScanNode>() {
							public int compare(BlockScanNode file1, BlockScanNode file2) {
								return comp.compare(file1.peek(), file2.peek());
			            }
			          });
		for(File tempFile: files) // populate PriorityQueue
			queue.add(new BlockScanNode(tempFile, schema)); 
		
		try 
		{
			File newFile = File.createTempFile("SortedFile", "", swapDir);
			BufferedWriter bw = new BufferedWriter(new FileWriter(newFile));
			while(!queue.isEmpty())
			{
				BlockScanNode scanOper = queue.poll();
				bw.write(DatumToString(scanOper.readOneTuple()));
				bw.newLine();
				
				if(scanOper.peek() == null) // if file is blank, delete the file
				{
					scanOper.f.delete();					
				}
				else // add the operator back in the queue
				{
					queue.add(scanOper);
				}
			}
			bw.close();
			return newFile;
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		return null;	
	}
	
}
