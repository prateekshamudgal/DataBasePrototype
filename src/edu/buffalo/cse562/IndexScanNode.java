package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.*;

import jdbm.PrimaryStoreMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

public class IndexScanNode extends PlanNode.Leaf  implements Operator
{
	List<ColumnDef> columnDef;
	String tableName;
	String columnName;
	RecordManager recordManager;
	SecondaryTreeMap<IndexRow, Long, IndexRow> indexNode;
	String indexName;

	// Create constructor using indexName and table schema
	// IndexName is of type TABLENAME_COLUMNNAME_IDX or TABLENAME_COLUMNNAME_PK
	// tableName and columnName can be obtained by splitting the indexName
	public IndexScanNode(String indexName, List<ColumnDef> colDef)
	{
		this.indexName = indexName;
		this.columnDef = colDef;
		this.columnName = indexName.split("_")[1];
		this.tableName = indexName.split("_")[0];
		reset();

		if(indexName.endsWith("_IDX"))
		{
			indexNode = getSecondaryIndexNode();
		}
		else if(indexName.endsWith("_PK"))
		{
			indexNode =  getSecondaryIndexNode();
		}
		else
		{
			indexNode = null;
		}
	}

	// Create constructor using tableName, index column and table schema
	// IndexName is of type TABLENAME_COLUMNNAME_IDX or TABLENAME_COLUMNNAME_PK
	public IndexScanNode(String tabName, List<ColumnDef> colDef, String colName)
	{
		reset();
		this.columnDef = colDef;
		this.columnName = colName;
		this.tableName = tabName;
		try 
		{
			recordManager = RecordManagerFactory.createRecordManager(Main.indexDir + "/" + tableName);

		} catch (IOException e) 
		{
			e.printStackTrace();
		}

		indexName = (tableName+"_"+colName).toUpperCase()+"_";
		if(Main.indexStorage.containsKey(indexName+"IDX"))
		{
			indexName = indexName+"IDX";
			indexNode = getSecondaryIndexNode();
		}
		else if(Main.indexStorage.containsKey(indexName+"PK"))
		{
			indexName = indexName+"PK";
			indexNode = getSecondaryIndexNode();
		}
		else
		{
			indexName = null;
			indexNode =  null;
		}
	}

	private SecondaryTreeMap<IndexRow, Long, IndexRow> getSecondaryIndexNode()
	{
		if(indexName == null)
			return null;

		IndexSerializer valueSerializer = new IndexSerializer(columnDef);
		final IndexAttributes idxAttr = Main.indexStorage.get(indexName);
		
		PrimaryStoreMap<Long, IndexRow> priStoreMap = recordManager.storeMap(tableName, valueSerializer);

		SecondaryTreeMap<IndexRow, Long, IndexRow> secTreeMap = 
				priStoreMap.secondaryTreeMap(idxAttr.getIndexName(),
				new SecondaryKeyExtractor<IndexRow, Long, IndexRow>() 
				{
					public IndexRow extractSecondaryKey(Long key, IndexRow row) 
					{
						return BuildIndexes.getKeyEntries(idxAttr.getKeyColumns(), row.data);
					}
				}, new KeyComparator(), idxAttr.getKeySerializer());
		return secTreeMap;
	}

	// Call this function when you have an equality condition
	// Datum d => the key to be matched
	// eg: A.x = 10 where indexName = A_x_IDX or A_x_PK  (already set through constructor)
	// Return => List of all matching tuples, or empty list if no tuples match
	public List<Datum[]> getMatchingTuples(Datum d)
	{
		if(indexNode == null)
			return null;
		
		List<Datum[]> list = new ArrayList<Datum[]>();
		Datum[] row = new Datum[1];
		row[0] = d;
		IndexRow key = new IndexRow(row);
		
		if(indexName == null || !indexNode.containsKey(key))
			return null;
		
		/*Iterator<Long> iter = indexNode.get(key).iterator();		
		if(indexName.endsWith("_IDX"))
		{
            for(Long recId: indexNode.get(key))
            {
                list.add(indexNode.getPrimaryValue(recId).data);
            }			
		}
		else if(indexName.endsWith("_PK"))
		{
			if(iter.hasNext())
                list.add(indexNode.getPrimaryValue(iter.next()).data);
		}*/
		for(Long recId: indexNode.get(key))
        {
            list.add(indexNode.getPrimaryValue(recId).data);
        }	
		return list;
	}

	// Call this function when you have a range condition
	// left, right => range conditions in any order
	// eg: A.x >= 10 and A.x < 20 
	// where indexName = A_x_IDX or A_x_PK (already set through constructor)
	// Return => List of all matching tuples, or empty list if no tuples match
	@SuppressWarnings("unchecked")
	public List<Datum[]> getRangeTuples(Expression left, Expression right)
	{
		if(indexNode == null)
			return null;
		
		IndexRow fromKey = null;
		boolean fromInclusive = true;
		IndexRow toKey = null;
		boolean toInclusive = false;
		// subMap includes LHS and excludes RHS
		
		Expression e1 =  ((BinaryExpression) left).getRightExpression();
		Datum[] data1 = new Datum[1];
		if(e1 instanceof Function) // date
		{			
			StringValue dt = (StringValue) ((Function) e1).getParameters().getExpressions().get(0);
			Datum d = new Datum.Dt(dt.getValue());
			data1[0] = d;
		}
		else
		{
			Evaluator eval = new Evaluator(e1);
			e1.accept(eval);
			Datum d = eval.getValue();
			data1[0] = d;
		}
		
		if(left instanceof GreaterThan || left instanceof GreaterThanEquals)
		{
			fromKey = new IndexRow(data1);
			if(left instanceof GreaterThan)
				fromInclusive = false; 
		}				  
		else if(left instanceof MinorThan || left instanceof MinorThanEquals)
		{
			toKey = new IndexRow(data1);
			if(left instanceof MinorThanEquals)
				toInclusive = true; 
		}
		
		Expression e2 =  ((BinaryExpression) right).getRightExpression();
		Datum[] data2 = new Datum[1];
		if(e2 instanceof Function) // date
		{			
			StringValue dt = (StringValue) ((Function) e2).getParameters().getExpressions().get(0);
			Datum d = new Datum.Dt(dt.getValue());
			data2[0] = d;
		}
		else
		{
			Evaluator eval = new Evaluator(e2);
			e2.accept(eval);
			Datum d = eval.getValue();
			data2[0] = d;
		}
		if(right instanceof GreaterThan || right instanceof GreaterThanEquals)
		{			  
			fromKey = new IndexRow(data2);
			if(right instanceof GreaterThan)
				fromInclusive = false; 
		}				  
		else if(right instanceof MinorThan || right instanceof MinorThanEquals)
		{
			toKey = new IndexRow(data2);
			if(right instanceof MinorThanEquals)
				toInclusive = true; 
		}

		List<Datum[]> list = new ArrayList<Datum[]>();
		SortedMap<IndexRow, Iterable<Long>> map = indexNode.subMap(fromKey, toKey);
		
		for(IndexRow i: map.keySet())
		{
			if(i.equals(fromKey) && !fromInclusive)
				continue;
			
			if(map.get(i) instanceof Iterable)
			{
				for(Long j: map.get(i))
					list.add(indexNode.getPrimaryValue(j).data);
			}
			/*else //if(map.get(i) instance of Long)
			{
				list.add(indexNode.getPrimaryValue(((Iterator<Long>) map.get(i)).next()).data);
			}	*/
		}
		if(toInclusive) // toKey is exclusive by default
		{
			List<Datum[]> tuples = getMatchingTuples(toKey.data[0]);
			if(tuples!=null)
			for(Datum[] dat: tuples)
				list.add(dat); 
		}
		return list;
	}

	// Call this function when you have relational conditions like >=, >, <=, <, =
	// exp => condition to be evaluated
	// Return => List of all matching tuples, or empty list if no tuples match
	@SuppressWarnings("unchecked")
	public List<Datum[]> getRangeTuples(Expression exp)
	{
		if(indexNode == null)
			return null;
		
		Expression e =  ((BinaryExpression) exp).getRightExpression();
		Datum[] data = new Datum[1];
		if(e instanceof Function) // date
		{			
			StringValue dt = (StringValue) ((Function) e).getParameters().getExpressions().get(0);
			Datum d = new Datum.Dt(dt.getValue());
			data[0] = d;
		}
		else
		{
			Evaluator eval = new Evaluator(e);
			e.accept(eval);
			Datum d = eval.getValue();
			data[0] = d;
		}

		IndexRow key = new IndexRow(data);
		boolean keyInclusive = true;		
		
		List<Datum[]> list = new ArrayList<Datum[]>();		
		if(exp instanceof GreaterThan || exp instanceof GreaterThanEquals)
		{
			if(exp instanceof GreaterThan)
				keyInclusive = false; 
			
			SortedMap<IndexRow, Iterable<Long>> map = indexNode.tailMap(key);
			
			for(IndexRow i: map.keySet())
			{
				if(i.equals(key) && !keyInclusive)
					continue;
				
				if(map.get(i) instanceof Iterable)
				{
					for(Long j: map.get(i))
						list.add(indexNode.getPrimaryValue(j).data);
				}
				/*else //if(map.get(i) instance of Long)
				{
					list.add(indexNode.getPrimaryValue(((Iterator<Long>) map.get(i)).next()).data);
				}*/
			}
		}				  
		else if(exp instanceof MinorThan || exp instanceof MinorThanEquals)
		{			
			if(exp instanceof MinorThanEquals)
				keyInclusive = true; 
			
			
			SortedMap<IndexRow, Iterable<Long>> map = indexNode.headMap(key);
			for(IndexRow i: map.keySet())
			{
				if(map.get(i) instanceof Iterable)
				{
					for(Long j: map.get(i))
						list.add(indexNode.getPrimaryValue(j).data);
				}
				/*else if(map.get(i) instanceof Long)
				{
					
					list.add(indexNode.getPrimaryValue(((Iterator<Long>) map.get(i)).next()).data);
				}	*/
			}
			if(keyInclusive) // toKey is exclusive by default
			{
				List<Datum[]> tuples = getMatchingTuples(key.data[0]);
				if(tuples!=null)
				for(Datum[] dat: tuples)
					list.add(dat);  
			}
		}
		else // EqualsTo
		{
			List<Datum[]> tuples = getMatchingTuples(key.data[0]);
			if(tuples!=null)
			for(Datum[] dat: tuples)
				list.add(dat); 
		}
		return list;
	}

	@Override
	public List<ColumnDef> getSchemaVars() {
		return this.columnDef;
	}

	@Override
	public Datum[] readOneTuple()
	{	
		return null;
	}

	@Override
	public void reset() // read the main file again
	{
		try {
			if(recordManager!=null)				
			{
				recordManager.close();
				recordManager.clearCache();
			}
			recordManager = RecordManagerFactory.createRecordManager(Main.indexDir + "/" + tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getTupleSize()
	{
		return 0;
	}
}

