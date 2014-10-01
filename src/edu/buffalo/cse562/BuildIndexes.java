package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import jdbm.InverseHashView;
import jdbm.PrimaryStoreMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

public class BuildIndexes {
	
	static File schemaFile = null;
	static File indexDir = null;
	static File dataDir = null;
	static KeyComparator keyComp = new KeyComparator();
	
	@SuppressWarnings("unchecked")
	public static void PreCompute(File dir, File idxDir, File schFile)
	{
		List<Index> indexes;
		indexDir = idxDir;
		schemaFile = schFile;
		dataDir = dir;
		try
		{			
			// clear existing indexes
			emptyIndexDirectory(indexDir); 
			
			//Start Reading the schema file
			FileReader stream = new FileReader(schemaFile);

			CCJSqlParser sqlStatementParser = new CCJSqlParser(stream);
			Statement sqlStatement;
						
			while((sqlStatement = sqlStatementParser.Statement()) != null)
			{
				if (sqlStatement instanceof CreateTable)
				{					
					CreateTable ct = (CreateTable) sqlStatement;
					String tableName = ct.getTable().getName().toUpperCase();
					
					//Set column Definition
					TableView tableView = new TableView();
					List<ColumnDefinition> colDef = ct.getColumnDefinitions();
					tableView.setColDef(colDef,tableName,"");
					
					// list of all indexes to be created for a table
					indexes = ct.getIndexes();
					List<IndexAttributes> indexList = BuildIndexList(indexes, tableName, tableView.getColumnDef());
					
					IndexSerializer valueSerializer = new IndexSerializer(tableView.getColumnDef());										
					
					// create the index
					Datum[] row = null;
					BlockScanNode scanner = new BlockScanNode(new File(dataDir, tableName + ".dat"), tableView.getColumnDef());
					RecordManager recMan = RecordManagerFactory.createRecordManager(indexDir+"/"+tableName);						
					
					PrimaryStoreMap <Long, IndexRow> psm = recMan.storeMap(tableName, valueSerializer);	
					//InverseHashView <Long, IndexRow> ihv = psm.inverseHashView(tableName+"Inv");
					
					List<SecondaryTreeMap<IndexRow, Long, IndexRow>> secTreeMaps = new ArrayList<SecondaryTreeMap<IndexRow, Long, IndexRow>>();										
					
					for(int i=0; i<indexList.size(); i++)
					{
						final IndexAttributes idxAttr = indexList.get(i);
						SecondaryTreeMap<IndexRow, Long, IndexRow> secTreeMap = psm.secondaryTreeMap(idxAttr.getIndexName(),
									new SecondaryKeyExtractor<IndexRow, Long, IndexRow>() 
									{
										public IndexRow extractSecondaryKey(Long key, IndexRow row) 
										{
											return getKeyEntries(idxAttr.getKeyColumns(), row.data);
										}
									}, new KeyComparator(), idxAttr.getKeySerializer());
						secTreeMaps.add(secTreeMap);						
					}
					
					int rowCount = 0;
					while (true)
					{
						row = scanner.readOneTuple();
						if (row == null) break;
						
						// add row to primary index
						// this primary index automatically updates secondary indexes
						IndexRow row1 = new IndexRow(row);
						psm.putValue(row1);		
						
						rowCount++;
					}						
					recMan.commit();
					recMan.clearCache();
					System.out.println("Size: " + psm.size() + " - RowCount: "+rowCount);
					recMan.close();
				}	
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static List<IndexAttributes> BuildIndexList(List<Index> indexes, String tableName, List<ColumnDef> colDef)
	{
		List<IndexAttributes> indexList = new ArrayList<IndexAttributes>();
		for(Index idx : indexes) 
		{						
			List<Integer> keyColumns = getColumnIndexes(idx.getColumnsNames(), colDef);
			List<ColumnDef> keySchema = getKeySchema(keyColumns, colDef);
			if(keySchema.size() > 1)
				continue;
			
			IndexAttributes indexAttr = new IndexAttributes();
			indexAttr.setIndexType(idx.getType()); // PRIMARY KEY, INDEX
			indexAttr.setKeyColumns(keyColumns);						
			indexAttr.setKeySchema(keySchema);												
			indexAttr.setKeySerializer(new IndexSerializer(keySchema));
			
			String indexName = null;						
			switch(idx.getType())
			{
				case "PRIMARY KEY":
					indexName = (tableName+"_"+keySchema.get(0).columnName).toUpperCase()+"_PK";
					break;
					
				case "INDEX":
					indexName = (tableName+"_"+keySchema.get(0).columnName).toUpperCase()+"_IDX";
					break;
			}
			indexAttr.setIndexName(indexName);
			indexList.add(indexAttr);
		
			//System.out.println(indexAttr.getIndexName());
			keyColumns = null;
			keySchema = null;
		}
		return indexList;
	}

	private static List<Integer> getColumnIndexes(List<String> keyCols, List<ColumnDef> colDef)
	{
		List<Integer> list = new ArrayList<Integer>();
		for(String key: keyCols)
		{
			for(int i=0; i<colDef.size(); i++)
			{
				if(colDef.get(i).getColumnName().equalsIgnoreCase(key))
				{
					list.add(i);
					break;
				}
			}			
		}
		return list;
	}
	
	public static IndexRow getKeyEntries(List<Integer> keyColumns, Datum[] row)
	{
		Datum[] key = new Datum[keyColumns.size()];
		for(int i=0; i<keyColumns.size(); i++)
			key[i] = row[keyColumns.get(i)];
		return new IndexRow(key);
	}
	
	private static List<ColumnDef> getKeySchema(List<Integer> keyColumns, List<ColumnDef> colDef)
	{
		List<ColumnDef> keySchema = new ArrayList<ColumnDef>();
		for(int i=0; i<keyColumns.size(); i++)
			keySchema.add(colDef.get(keyColumns.get(i)));
		return keySchema;
	}
	
	private static void emptyIndexDirectory(File file)
	{		        
        String[] myFiles;      
        if(file.isDirectory())
        {  
            myFiles = file.list();  
            for (int i=0; i<myFiles.length; i++) 
            {  
                File myFile = new File(file, myFiles[i]);   
                myFile.delete();  
            }  
         }
	}
}