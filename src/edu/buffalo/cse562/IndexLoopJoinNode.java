package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class IndexLoopJoinNode extends PlanNode.Binary implements Operator 
{	  
	public Expression joinExpression;
	PlanNode joinedPathNode = null; // final joined file
	static File swapDir = null;
	TableView hashJoinResult = new TableView();
	TableView indexJoinResult = new TableView();
	List<ColumnDef> joinSchema;
	BufferedWriter buffw;
	File f;
	int partitionNum;
	int secondPartitionNum;
		  
	public static IndexLoopJoinNode make(PlanNode lhs, PlanNode rhs, Expression joinExpression){
		IndexLoopJoinNode indexJoin = new IndexLoopJoinNode();
		//lhs is a SelectionNode, rhs is a IndexScanNode
		indexJoin.setLHS(lhs);
		indexJoin.setRHS(rhs);
		//indexJoin.joinSchema = indexJoin.getSchemaVars();
		indexJoin.setSchemaVars();
		indexJoin.joinExpression = joinExpression;
		
		/*if(joinExpression != null)
		{
			List<Column> leftCol= new ArrayList<Column>();
			leftCol.add((Column) (((BinaryExpression) joinExpression).getLeftExpression()));
			List<Column> rightCol= new ArrayList<Column>();
			rightCol.add((Column) (((BinaryExpression) joinExpression).getRightExpression()));
			
			String leftTable = ((Column) ((BinaryExpression) joinExpression).getLeftExpression()).getTable().getName().toUpperCase();
			if (rhs.getSchemaVars().get(0).tableName.equalsIgnoreCase(leftTable))
			{
				List<Column> tmp = new ArrayList<Column>();
				tmp = leftCol;
				leftCol = rightCol;
				rightCol = tmp;				
			}
			
			//PartitionNode
			PlanNode hashJoinLhs = PartitionNode.make(lhs, leftCol, swapDirectory, hashJoin.partitionNum);
			hashJoin.setLHS(hashJoinLhs);
			
			PlanNode hashJoinRhs = PartitionNode.make(rhs, rightCol, swapDirectory, hashJoin.partitionNum);
			hashJoin.setRHS(hashJoinRhs);
		}
		else
		{
			PlanNode hashJoinLhs = SelectionNode.make(lhs, null);
			hashJoin.setLHS(hashJoinLhs);
			
			PlanNode hashJoinRhs = SelectionNode.make(rhs, null);
			hashJoin.setRHS(hashJoinRhs);
		}*/
		
		indexJoin.reset();
		indexJoin.getResult();		
		return indexJoin;
	}
	
	public void getResult()
	{
		List<ColumnDef> leftColDef = this.lhs.getSchemaVars();
		List<ColumnDef> rightColDef = this.rhs.getSchemaVars();
		if (joinExpression != null)
		{
			String leftTable = ((Column) ((BinaryExpression) joinExpression).getLeftExpression()).getTable().getName().toUpperCase();
			String rightTable = ((Column) ((BinaryExpression) joinExpression).getRightExpression()).getTable().getName().toUpperCase();
			String leftCol = ((Column) ((BinaryExpression) joinExpression).getLeftExpression()).getColumnName().toUpperCase();
			String rightCol = ((Column) ((BinaryExpression) joinExpression).getRightExpression()).getColumnName().toUpperCase();
			if (rightColDef.get(0).tableName.equalsIgnoreCase(leftTable))
			{
				String tmp = leftTable;
				leftTable = rightTable;
				rightTable = tmp;
				tmp = leftCol;
				leftCol = rightCol;
				rightCol = tmp;
			}
			
			int leftColumnIndex = getColmnIndex(leftTable, leftCol, this.lhs.getSchemaVars());
			int rightColumnIndex = getColmnIndex(rightTable, rightCol, this.rhs.getSchemaVars());
			
			Datum [] tuple_lhs = this.lhs.readOneTuple();
			while (tuple_lhs != null)
			{
				List<Datum[]> tuples_rhs = ((IndexScanNode)this.rhs).getMatchingTuples(tuple_lhs[leftColumnIndex]);
				if(tuples_rhs != null) {
					for (int i = 0; i < tuples_rhs.size(); i++) {
						Datum[] tuple_com = new Datum[leftColDef.size() + rightColDef.size()];
						for (int k = 0; k < leftColDef.size(); k ++)
							tuple_com[k] = tuple_lhs[k];
						for (int k = 0; k < rightColDef.size(); k ++)
							tuple_com[k + leftColDef.size()] = tuples_rhs.get(i)[k];
						this.indexJoinResult.addTuple(tuple_com);
					}
				}
				tuple_lhs = this.lhs.readOneTuple();
			}
		}
	}
	
	@Override
	public Datum[] readOneTuple()
	{
		if(swapDir == null)
			return indexJoinResult.readOneTuple();
		else
			return joinedPathNode.readOneTuple();
	}
	
	private String DatumToString(Datum[] row)
	{
		String line = "";
		for(int i=0; i<row.length-1; i++)
			line += row[i].toString() + "|";
		line += row[row.length-1].toString();
		return line;
	}
	
	private int getColmnIndex(String tName, String colmName, List<ColumnDef> cols)
	{
		if(tName==null || "".equals(tName))
		{
			for(int i=0; i< cols.size(); i++)
				if(cols.get(i).getColumnName().equalsIgnoreCase(colmName))
					return i;
		}
		else
		{
			for(int i=0; i< cols.size(); i++)
				if(cols.get(i).getColumnName().equalsIgnoreCase(colmName) && cols.get(i).getTableName().equalsIgnoreCase(tName))
					return i;
		}
		return -1;
	}

	@Override
	public void reset() {
		this.lhs.reset();
		//this.rhs.reset();	
	}


	public void setSchemaVars() 
	{
		List<ColumnDef> schema_lhs = this.lhs.getSchemaVars();
		List<ColumnDef> schema_rhs = this.rhs.getSchemaVars();
		this.joinSchema = new ArrayList<ColumnDef>();
		for (int i = 0; i < schema_lhs.size(); i ++)
			this.joinSchema.add(schema_lhs.get(i));
		for (int i = 0; i < schema_rhs.size(); i ++)
			this.joinSchema.add(schema_rhs.get(i));
	}
	
	public List<ColumnDef> getSchemaVars() 
	{
		/*List<ColumnDef> schema_lhs = this.lhs.getSchemaVars();
		List<ColumnDef> schema_rhs = this.rhs.getSchemaVars();
		List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
		for (int i = 0; i < schema_lhs.size(); i ++)
			newSchema.add(schema_lhs.get(i));
		for (int i = 0; i < schema_rhs.size(); i ++)
			newSchema.add(schema_rhs.get(i));
		return newSchema;*/
		return this.joinSchema;
	}
	
	public void setSchema(List<ColumnDef> newSchema)
	{
		this.joinSchema = newSchema;
	}
	
	public void modifyLastPartSchema(List<ColumnDef> alais_schema) 
	{
		String tableName = alais_schema.get(0).tableName;
		for (int i = 0; i < alais_schema.size(); i ++)
			this.joinSchema.get(this.joinSchema.size() - alais_schema.size() + i).setTableName(tableName);
		
	}
	
}
