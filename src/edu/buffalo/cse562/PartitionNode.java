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

public class PartitionNode extends PlanNode.Unary implements Operator {
	List<?> orderByElements;	
	List<ColumnDef> schema;
	
	TableView resultTable = new TableView(); // For storing in memory when swapDir = false
	PlanNode sortedFileScanner = null; // For storing on disk when swapDir = true
	File swapDir = null;
	List<TableView> partitionTableList = new ArrayList<TableView>();
	List<PlanNode> partitionFileScanner = null;
	int partitionNum;
	
	public PartitionNode(List<?> orderByElements, File swapDirectory, int partitionNum)
	{ 
		this.orderByElements = orderByElements; 
		swapDir = swapDirectory;
		this.partitionNum = partitionNum;
	}
		
	public static PartitionNode make(PlanNode source, List<?> orderByElements, File swapDir, int partitionNum)
    {
		PartitionNode partition = new PartitionNode(orderByElements, swapDir, partitionNum);
		partition.setChild(source);
		partition.schema = partition.getSchemaVars();
		partition.partitionFileScanner = new ArrayList<PlanNode>();
		partition.partitionFile();
		return partition;		
    }
		
	private void partitionFile()
	{
		PlanNode source = getChild();

		//Get the index of the partition column
		int[] idx_order = new int[orderByElements.size()];
		int idx = 0;
		for (int i = 0; i < orderByElements.size(); i++)
		{
			//isAsc[i] = true;
			for (int j = 0; j < this.schema.size(); j++)
			{
				Column colName;
				if (orderByElements.get(i) instanceof OrderByElement)
				     colName = (Column) ((OrderByElement) orderByElements.get(i)).getExpression();
				else 
					colName = (Column)orderByElements.get(i);
				
				String tabName = colName.getTable().getName(); // OrderBy TableName
				String colmName = colName.getColumnName(); // OrderBy ColumnName
				ColumnDef cDef = this.schema.get(j);
				
				if(tabName==null || "".equals(tabName))
				{
					if (colmName.equalsIgnoreCase(cDef.columnName))
					{
						idx_order[idx ++] = j;
					}	
				}
				else
				{						
					if (colmName.equalsIgnoreCase(cDef.columnName) && tabName.equalsIgnoreCase(cDef.tableName))
					{
						idx_order[idx ++] = j;
					}					
				}
			}
		}
		//Assume orderByElement only has one element.
		Datum[] row = source.readOneTuple();
		int bucket_label;
		try
		{
			List<File> fileListPartition = new ArrayList<File>();
			List<BufferedWriter>  bwListPartition = new ArrayList<BufferedWriter>();
			for (int i = 0; i < partitionNum; i ++)
			{
				fileListPartition.add(i, File.createTempFile(this.schema.get(0).tableName + "_Partition_" + i, "", swapDir));
				bwListPartition.add(new BufferedWriter(new FileWriter(fileListPartition.get(i))));
			}
			for (int i = 0; i < partitionNum; i ++)
			{
				partitionTableList.add(new TableView());
				partitionTableList.get(i).setColDef(source.getSchemaVars());
			}
			while(row != null)
			{
				//Assume orderByElement only has one element
				bucket_label = row[idx_order[0]].hashCode() % partitionNum;
				if (swapDir != null)
				{
					bwListPartition.get(bucket_label).write(DatumToString(row));
					bwListPartition.get(bucket_label).newLine();
				}
				else
				{
					partitionTableList.get(bucket_label).addTuple(row);
				}
				row = source.readOneTuple();
			}
			for (int i = 0; i < partitionNum; i ++)
			{
				bwListPartition.get(i).close();
				partitionFileScanner.add(new ScanNode(fileListPartition.get(i), this.schema));
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	@Override
	// Not used
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
	
	
	
	public List<ColumnDef> getSchemaVars() {
		return getChild().getSchemaVars();
	}
	
	private String DatumToString(Datum[] row)
	{
		String line = "";
		for(int i=0; i<row.length-1; i++)
			line += row[i].toString() + "|";
		line += row[row.length-1].toString();
		return line;
	}	
}
