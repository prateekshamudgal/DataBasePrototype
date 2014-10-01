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

public class HashJoinNode extends PlanNode.Binary implements Operator 
{	  
	public Expression joinExpression;
	PlanNode joinedPathNode = null; // final joined file
	static File swapDir = null;
	TableView hashJoinResult = new TableView();
	List<ColumnDef> joinSchema;
	BufferedWriter buffw;
	File f;
	int partitionNum;
	int secondPartitionNum;
		  
	public static HashJoinNode make(PlanNode lhs, PlanNode rhs, Expression joinExpression, File swapDirectory){
		HashJoinNode hashJoin = new HashJoinNode();
		hashJoin.partitionNum = 10;
		hashJoin.secondPartitionNum = 9000;
		if(joinExpression != null)
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
		}
		hashJoin.joinSchema = hashJoin.getSchemaVars();
		hashJoin.reset();
		hashJoin.joinExpression = joinExpression;
		swapDir = swapDirectory;
		hashJoin.getResult();		
		return hashJoin;
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
			
			//Both leftPartitionNode and rightPartitionNode will return a file list.
			//Read in partition of R, hash it using h2
			//Scan matching partition of S, search for matches
			try 
			{			
				f = File.createTempFile("HashJoin_", "", swapDir);
				buffw = new BufferedWriter(new FileWriter(f));
				//If all is in memory, then store it in memory
				hashJoinResult.setColDef(getSchemaVars());
				
				for (int i = 0; i < this.partitionNum; i ++)
				{
					//Read lhs, second partition
					HashMap<Integer, List<Datum []>> secondHashTable = new HashMap<Integer, List<Datum []>>();
					if (swapDir != null) 
					{
						if (((PartitionNode) this.lhs).partitionFileScanner.get(i) != null)
						{
							//Build a hashTable for lhs partition i
							Datum [] tuple_lhs = ((PartitionNode) this.lhs).partitionFileScanner.get(i).readOneTuple();
							while (tuple_lhs != null)
							{
								int hashCode = 1;
								hashCode = hashCode * 17 + tuple_lhs[leftColumnIndex].hashCode();
								hashCode = (hashCode * 13) % this.secondPartitionNum;
								if (secondHashTable.containsKey(hashCode))
									secondHashTable.get(hashCode).add(tuple_lhs);
								else
								{
									List<Datum[]> tmp_list = new ArrayList<Datum[]>();
									tmp_list.add(tuple_lhs);
									secondHashTable.put(hashCode, tmp_list);
								}
								tuple_lhs = ((PartitionNode) this.lhs).partitionFileScanner.get(i).readOneTuple();
							}
							//Probe phase: read one tuple from rhs, and join
							if (((PartitionNode) this.rhs).partitionFileScanner.get(i) != null)
							{
								Datum [] tuple_rhs = ((PartitionNode) this.rhs).partitionFileScanner.get(i).readOneTuple();
								while (tuple_rhs != null)
								{
									int hashCode = 1;
									hashCode = hashCode * 17 + tuple_rhs[rightColumnIndex].hashCode();
									hashCode = (hashCode * 13) % this.secondPartitionNum;
									if (secondHashTable.containsKey(hashCode))
									{
										for(int j = 0; j < secondHashTable.get(hashCode).size(); j ++)
										{
											Datum[] tuple_lhs_local = secondHashTable.get(hashCode).get(j);
											if (tuple_lhs_local[leftColumnIndex].compareTo(tuple_rhs[rightColumnIndex]) == 0)
											{
												Datum[] tuple_com = new Datum[leftColDef.size() + rightColDef.size()];
												for (int k = 0; k < leftColDef.size(); k ++)
													tuple_com[k] = tuple_lhs_local[k];
												for (int k = 0; k < rightColDef.size(); k ++)
													tuple_com[k + leftColDef.size()] = tuple_rhs[k];
												
												if (swapDir == null)
												{
													hashJoinResult.addTuple(tuple_com);
												}
												else
												{
													buffw.write(DatumToString(tuple_com));
													buffw.newLine();
												}
											}
										}
									}
									tuple_rhs = ((PartitionNode) this.rhs).partitionFileScanner.get(i).readOneTuple();
								}	
							}
						}
					}
					else 
					{
						//Now everything is in memory.
						if (((PartitionNode) this.lhs).partitionTableList.get(i) != null)
						{
							//Build a hashTable for lhs partition i
							Datum [] tuple_lhs = ((PartitionNode) this.lhs).partitionTableList.get(i).readOneTuple();
							while (tuple_lhs != null)
							{
								int hashCode = 1;
								hashCode = hashCode * 17 + tuple_lhs[leftColumnIndex].hashCode();
								hashCode = (hashCode * 13) % this.secondPartitionNum;
								if (secondHashTable.containsKey(hashCode))
									secondHashTable.get(hashCode).add(tuple_lhs);
								else
								{
									List<Datum[]> tmp_list = new ArrayList<Datum[]>();
									tmp_list.add(tuple_lhs);
									secondHashTable.put(hashCode, tmp_list);
								}
								tuple_lhs = ((PartitionNode) this.lhs).partitionTableList.get(i).readOneTuple();
							}
							//Probe phase: read one tuple from rhs, and join
							if (((PartitionNode) this.rhs).partitionTableList.get(i) != null)
							{
								Datum [] tuple_rhs = ((PartitionNode) this.rhs).partitionTableList.get(i).readOneTuple();
								while (tuple_rhs != null)
								{
									int hashCode = 1;
									hashCode = hashCode * 17 + tuple_rhs[rightColumnIndex].hashCode();
									hashCode = (hashCode * 13) % this.secondPartitionNum;
									if (secondHashTable.containsKey(hashCode))
									{
										for(int j = 0; j < secondHashTable.get(hashCode).size(); j ++)
										{
											Datum[] tuple_lhs_local = secondHashTable.get(hashCode).get(j);
											if (tuple_lhs_local[leftColumnIndex].compareTo(tuple_rhs[rightColumnIndex]) == 0)
											{
												Datum[] tuple_com = new Datum[leftColDef.size() + rightColDef.size()];
												for (int k = 0; k < leftColDef.size(); k ++)
													tuple_com[k] = tuple_lhs_local[k];
												for (int k = 0; k < rightColDef.size(); k ++)
													tuple_com[k + leftColDef.size()] = tuple_rhs[k];
												
												if (swapDir == null)
												{
													hashJoinResult.addTuple(tuple_com);
												}
												else
												{
													buffw.write(DatumToString(tuple_com));
													buffw.newLine();
												}
											}
										}
									}
									tuple_rhs = ((PartitionNode) this.rhs).partitionTableList.get(i).readOneTuple();
								}	
							}
						}
					}
				}
				buffw.close();
				joinedPathNode = new ScanNode(f, getSchemaVars());
			} catch (IOException e) 
			{
				try {
					buffw.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		else
		{
			try 
			{
				f = File.createTempFile("HashJoin_", "", swapDir);
				buffw = new BufferedWriter(new FileWriter(f));
				for (int j = 0; j < this.partitionNum; j ++)
				{
					//Didn't consider the case of cross product in memory
					Datum[] tuple_lhs = ((PartitionNode) this.lhs).partitionFileScanner.get(j).readOneTuple();
					Datum[] tuple_rhs = ((PartitionNode) this.rhs).partitionFileScanner.get(j).readOneTuple();
					while (tuple_lhs != null)
					{
						while (tuple_rhs != null)
						{
							Datum[] tuple_com = new Datum[leftColDef.size() + rightColDef.size()];
							for (int i = 0; i < leftColDef.size(); i ++)
								tuple_com[i] = tuple_lhs[i];
							for (int i = 0; i < rightColDef.size(); i ++)
								tuple_com[i + leftColDef.size()] = tuple_rhs[i];
							if (swapDir == null)
							{
								hashJoinResult.addTuple(tuple_com);
							}
							else
							{
								buffw.write(DatumToString(tuple_com));
								buffw.newLine();
							}
							tuple_rhs = ((PartitionNode) this.rhs).partitionFileScanner.get(j).readOneTuple();
						}
						// I am not sure whether using this.rhs.reset() can read this.lhs from beginning.
						this.rhs.reset();
						tuple_lhs = ((PartitionNode) this.lhs).partitionFileScanner.get(j).readOneTuple();
						tuple_rhs = ((PartitionNode) this.rhs).partitionFileScanner.get(j).readOneTuple();
					}
				}
				buffw.close();
				joinedPathNode = new ScanNode(f, getSchemaVars());
			} catch (IOException e) 
			{
				try {
					buffw.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}				
		}
	}
	
	@Override
	public Datum[] readOneTuple()
	{
		if(swapDir == null)
			return hashJoinResult.readOneTuple();
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
		this.rhs.reset();	
	}

	@Override
	public List<ColumnDef> getSchemaVars() 
	{
		List<ColumnDef> schema_lhs = this.lhs.getSchemaVars();
		List<ColumnDef> schema_rhs = this.rhs.getSchemaVars();
		List<ColumnDef> newSchema = new ArrayList<ColumnDef>();
		for (int i = 0; i < schema_lhs.size(); i ++)
			newSchema.add(schema_lhs.get(i));
		for (int i = 0; i < schema_rhs.size(); i ++)
			newSchema.add(schema_rhs.get(i));
		return newSchema;
	}
}
