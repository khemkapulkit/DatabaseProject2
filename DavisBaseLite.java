import java.io.RandomAccessFile;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;



/**
 * @author Chris Irwin Davis
 * @version 1.0
 * <b>This is an example of how to read/write binary data files using RandomAccessFile class</b>
 *
 */
public class DavisBaseLite {
	// This can be changed to whatever you like
	static String prompt = "davisql> ";
	
	/*
	 *  This example does not dynamically load a table schema to be able to 
	 *  read/write any table -- there is exactly ONE hardcoded table schema.
	 *  These are the variables associated with that hardecoded table schema.
	 *  Your database engine will need to define these on-the-fly from
	 *  whatever table schema you use from your information_schema
	 */
	static String widgetTableFileName = "widgets.dat";
	static String tableIdIndexName = "widgets.id.ndx";
	static int id;
	static String name;
	static short quantity;
	static float probability;
	static String ActiveSchema = "information_schema" ;
	

    public static void main(String[] args) throws IOException {
		/* Display the welcome splash screen */
		splashScreen();
		
		/* 
		 *  Manually create a binary data file for the single hardcoded 
		 *  table. It inserts 5 hardcoded records. The schema is inherent 
		 *  in the code, pre-defined, and static.
		 *  
		 *  An index file for the ID field is created at the same time.
		 */
		hardCodedCreateTableWithIndex();

		/* 
		 *  The Scanner class is used to collect user commands from the prompt
		 *  There are many ways to do this. This is just one.
		 *
		 *  Each time the semicolon (;) delimiter is entered, the userCommand String
		 *  is re-populated.
		 */
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String userCommand; // Variable to collect user input from the prompt

		do {  // do-while !exit
			System.out.print(prompt);
			userCommand = scanner.next().trim().toLowerCase();
			//System.out.print(userCommand);
			/*
			 *  This switch handles a very small list of commands of known syntax.
			 *  You will probably want to write a parse(userCommand) method to
			 *  to interpret more complex commands. 
			 */
			switch (userCommand) {
				case "show schemas":
					showSchemas();
					break;
				case "display all":
					displayAllRecords();
					break;
				case "display":
					/* 
					 *  Your record retrieval must use the SELECT-FROM-WHERE syntax
					 *  This simple syntax allows retrieval of single records based 
					 *  only on the ID column.
					 */
					String recordID = scanner.next().trim();
					displayRecordID(Integer.parseInt(recordID));
					break;
				case "help":
					help();
					break;
				case "version":
					version();
					break;
				default:
				{
					if(userCommand.toLowerCase().contains("create schema"))
					{
						String SchemaName = userCommand.split(" ")[2];
						createSchema(SchemaName);
					}
					else 
					{ 
						if(userCommand.toLowerCase().contains("use schema"))
						{
							String SchemaName = userCommand.split(" ")[2];
							useSchema(SchemaName);
						}
						else 
						{ 
							if(userCommand.toLowerCase().contains("create table"))
							{
								createTable(userCommand);
							}
							else
							{
								if(userCommand.toLowerCase().contains("insert into"))
								{
									insert(userCommand);
								}
								else
								{

									if(userCommand.toLowerCase().contains("select"))
									{
										try {
											select(userCommand);
										} catch (Exception e) {
											// TODO Auto-generated catch block
											//e.printStackTrace();
											//System.out.println("Command Not Correct");
										}
									}
									else
									{
										System.out.println("I didn't understand the command: \"" + userCommand + "\"");
									}
								}
							}
						}
					}
				}
			}
		} while(!userCommand.equals("exit"));
		System.out.println("Exiting...");
	    
    } /* End main() method */


//  ===========================================================================
//  STATIC METHOD DEFINTIONS BEGIN HERE
//  ===========================================================================

    public static void select(String userCommand) throws Exception {
    	userCommand = userCommand.toLowerCase().replace("select * from", "").trim();
    	String TableName = userCommand.trim().split(" ")[0].trim();
    	
    	if(userCommand.contains("where")){
    		String condition = userCommand.trim().split("where", 2)[1].trim();
    		if(condition.contains("=")){
    			String column_field = condition.split("=")[0].trim();
    			String column_value = condition.split("=")[1].trim().replace("'","");
    			RandomAccessFile TableFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+TableName+".tbl", "rw");
				RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema/information_schema.columns.tbl", "rw");
				RandomAccessFile IndexFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+TableName+"."+column_field+".ndx", "rw");
				ArrayList<ColumnDetails> A = new ArrayList<ColumnDetails>();
				ColumnDetails ColumnToCompare = null ;
				try{
				while(true){
					ColumnDetails cd = new ColumnDetails();
					byte varcharLength = columnsTableFile.readByte();
					String TableSchema = "";
					for(int i = 0; i < varcharLength; i++)
						TableSchema += (char)columnsTableFile.readByte();
					byte varcharLength1 = columnsTableFile.readByte();
					String table = "";
					for(int i = 0; i < varcharLength1; i++)
						table += (char)columnsTableFile.readByte();
					byte varcharLength2 = columnsTableFile.readByte();
					String column = "";
					for(int i = 0; i < varcharLength2; i++)
						column += (char)columnsTableFile.readByte();
					int OP = columnsTableFile.readInt();
					byte varcharLength3 = columnsTableFile.readByte();
					String columnType = "";
					for(int i = 0; i < varcharLength3; i++)
						columnType += (char)columnsTableFile.readByte();
					byte varcharLength4 = columnsTableFile.readByte();
					String nullable = "";
					for(int i = 0; i < varcharLength4; i++)
						nullable += (char)columnsTableFile.readByte();
					byte varcharLength5 = columnsTableFile.readByte();
					String key = "";
					for(int i = 0; i < varcharLength5; i++)
						key += (char)columnsTableFile.readByte();
					cd.Column_Name = column;
					cd.Column_Type = columnType;
					cd.Ordinal_Position = OP;
					cd.IsNullable = nullable;
					cd.Key = key;
					if(TableName.equalsIgnoreCase(table)){
						A.add(cd);
						if(column.equals(column_field)){
							ColumnToCompare = cd;
						}
					}
					
					}
				}
				catch(Exception e){}
				Collections.sort(A, new Comparator<ColumnDetails>() {
			        @Override
			        public int compare(ColumnDetails cd1, ColumnDetails cd2)
			        {

			        	   Integer od1 = cd1.Ordinal_Position;
			        	    Integer od2 = cd2.Ordinal_Position;
			        	    int compare = (int) Math.signum((od1).compareTo(od2));
			        	    return compare;
			        }
			        
			    });
				ArrayList offsets = null;
				TreeMap tm = getMap(TableName, column_field, ColumnToCompare.Column_Type );
				String columnType = ColumnToCompare.Column_Type;
				if(columnType.toLowerCase().contains("byte")){
					offsets = (ArrayList) tm.get(Byte.parseByte(column_value));
					
				}
				else if(columnType.toLowerCase().contains("short")){
					offsets = (ArrayList) tm.get(Short.parseShort(column_value));
				}
				else if(columnType.toLowerCase().contains("int")){
					offsets = (ArrayList) tm.get(Integer.parseInt(column_value));
				}
				else if(columnType.toLowerCase().contains("long")){
					offsets = (ArrayList) tm.get(Long.parseLong(column_value));
				}
				else if(columnType.toLowerCase().contains("char")){
					offsets = (ArrayList) tm.get(column_value);
				}
				else if(columnType.toLowerCase().contains("float")){
					offsets = (ArrayList) tm.get(Float.parseFloat(column_value));
				}
				else if(columnType.toLowerCase().contains("double")){
					offsets = (ArrayList) tm.get(Double.parseDouble(column_value));
				}
				else if(columnType.toLowerCase().contains("datetime")){
					offsets = (ArrayList) tm.get(column_value);
				}
				else if(columnType.toLowerCase().contains("date")){
					offsets = (ArrayList) tm.get(column_value+"__00:00:00");
				}
				 for (int j = 0; j < A.size(); j++){
		            	ColumnDetails cd = A.get(j);
		            	System.out.print(cd.Column_Name+ "\t");
		            }
		            System.out.println();
				for (int i = 0; i < offsets.size(); i++) {
					Long l = new Long((Integer)offsets.get(i));
		            TableFile.seek(l);
		            for (int j = 0; j < A.size(); j++){
		            	ColumnDetails cd = A.get(j);
		            	if(cd.Column_Type.toLowerCase().contains("byte")){
							System.out.print(TableFile.readByte()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("short")){
							System.out.print(TableFile.readShort()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("int")){
							System.out.print(TableFile.readInt()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("long")){
							System.out.print(TableFile.readLong()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("char")){
							byte varcharLength = TableFile.readByte();
							String S = "";
							for(int k = 0; k < varcharLength; k++)
								S += (char)TableFile.readByte();
							System.out.print(S+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("float")){
							System.out.print(TableFile.readFloat()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("double")){
							System.out.print(TableFile.readDouble()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("datetime")){
							String S = "";
							for(int k = 0; k < 19; k++)
								S += (char)TableFile.readByte();
							System.out.print(S+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("date")){
							String S = "";
							for(int k = 0; k < 19; k++)
								S += (char)TableFile.readByte();
							System.out.print(S.replace("_00:00:00", "")+"   ");
						}
		            	
		            }
		            System.out.println();
		        }
				
    		}
    		else if(condition.contains("<")){
    			String column_field = condition.split("<")[0].trim();
    			String column_value = condition.split("<")[1].trim().replace("'","");
    			RandomAccessFile TableFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+TableName+".tbl", "rw");
				RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema/information_schema.columns.tbl", "rw");
				RandomAccessFile IndexFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+TableName+"."+column_field+".ndx", "rw");
				ArrayList<ColumnDetails> A = new ArrayList<ColumnDetails>();
				ColumnDetails ColumnToCompare = null ;
				try{
				while(true){
					ColumnDetails cd = new ColumnDetails();
					byte varcharLength = columnsTableFile.readByte();
					String TableSchema = "";
					for(int i = 0; i < varcharLength; i++)
						TableSchema += (char)columnsTableFile.readByte();
					byte varcharLength1 = columnsTableFile.readByte();
					String table = "";
					for(int i = 0; i < varcharLength1; i++)
						table += (char)columnsTableFile.readByte();
					byte varcharLength2 = columnsTableFile.readByte();
					String column = "";
					for(int i = 0; i < varcharLength2; i++)
						column += (char)columnsTableFile.readByte();
					int OP = columnsTableFile.readInt();
					byte varcharLength3 = columnsTableFile.readByte();
					String columnType = "";
					for(int i = 0; i < varcharLength3; i++)
						columnType += (char)columnsTableFile.readByte();
					byte varcharLength4 = columnsTableFile.readByte();
					String nullable = "";
					for(int i = 0; i < varcharLength4; i++)
						nullable += (char)columnsTableFile.readByte();
					byte varcharLength5 = columnsTableFile.readByte();
					String key = "";
					for(int i = 0; i < varcharLength5; i++)
						key += (char)columnsTableFile.readByte();
					cd.Column_Name = column;
					cd.Column_Type = columnType;
					cd.Ordinal_Position = OP;
					cd.IsNullable = nullable;
					cd.Key = key;
					if(TableName.equalsIgnoreCase(table)){
						A.add(cd);
						if(column.equals(column_field)){
							ColumnToCompare = cd;
						}
					}
					
					}
				}
				catch(Exception e){}
				Collections.sort(A, new Comparator<ColumnDetails>() {
			        @Override
			        public int compare(ColumnDetails cd1, ColumnDetails cd2)
			        {

			        	   Integer od1 = cd1.Ordinal_Position;
			        	    Integer od2 = cd2.Ordinal_Position;
			        	    int compare = (int) Math.signum((od1).compareTo(od2));
			        	    return compare;
			        }
			        
			    });
				ArrayList offsets = null;
				TreeMap tm = getMap(TableName, column_field, ColumnToCompare.Column_Type );
				String columnType = ColumnToCompare.Column_Type;
				if(columnType.toLowerCase().contains("byte")){
					System.out.print("condition not correct");
					
				}
				else if(columnType.toLowerCase().contains("short")){
					offsets = new ArrayList<Short>();
					Set keys = tm.keySet();
					   for (Iterator i = keys.iterator(); i.hasNext();) {
					     Short key = (Short) i.next();
					     if(key < Integer.parseInt(column_value))
					     {
					    	 ArrayList all = (ArrayList) tm.get(key);
					    	 
					    	 for (int i1 = 0; i1 < all.size(); i1++) {
					    		 //System.out.println(all.get(i1));
					    		    offsets.add( (Integer) all.get(i1)); 
					    		}
					    	// offsets.addAll((ArrayList) tm.get(key));
						     
					     }
					   }
				}
				else if(columnType.toLowerCase().contains("int")){
					offsets = new ArrayList<Integer>();
					Set keys = tm.keySet();
					   for (Iterator i = keys.iterator(); i.hasNext();) {
					     Integer key = (Integer) i.next();
					     if(key < Integer.parseInt(column_value))
					     {
					    	 ArrayList all = (ArrayList) tm.get(key);
					    	 
					    	 for (int i1 = 0; i1 < all.size(); i1++) {
					    		 //System.out.println(all.get(i1));
					    		    offsets.add( (Integer) all.get(i1)); 
					    		}
					    	// offsets.addAll((ArrayList) tm.get(key));
						     
					     }
					   }
				}
				else if(columnType.toLowerCase().contains("long")){
					System.out.print("condition not correct");
				}
				else if(columnType.toLowerCase().contains("char")){
					System.out.print("condition not correct");
				}
				else if(columnType.toLowerCase().contains("float")){
					System.out.print("condition not correct");
				}
				else if(columnType.toLowerCase().contains("double")){
					System.out.print("condition not correct");
				}
				else if(columnType.toLowerCase().contains("datetime")){
					System.out.print("condition not correct");
				}
				else if(columnType.toLowerCase().contains("date")){
					System.out.print("condition not correct");
				}
				 for (int j = 0; j < A.size(); j++){
		            	ColumnDetails cd = A.get(j);
		            	System.out.print(cd.Column_Name+ "\t");
		            }
		            System.out.println();
				for (int i = 0; i < offsets.size(); i++) {
					Long l = new Long((Integer)offsets.get(i));
		            TableFile.seek(l);
		            for (int j = 0; j < A.size(); j++){
		            	ColumnDetails cd = A.get(j);
		            	if(cd.Column_Type.toLowerCase().contains("byte")){
							System.out.print(TableFile.readByte()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("short")){
							System.out.print(TableFile.readShort()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("int")){
							System.out.print(TableFile.readInt()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("long")){
							System.out.print(TableFile.readLong()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("char")){
							byte varcharLength = TableFile.readByte();
							String S = "";
							for(int k = 0; k < varcharLength; k++)
								S += (char)TableFile.readByte();
							System.out.print(S+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("float")){
							System.out.print(TableFile.readFloat()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("double")){
							System.out.print(TableFile.readDouble()+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("datetime")){
							String S = "";
							for(int k = 0; k < 19; k++)
								S += (char)TableFile.readByte();
							System.out.print(S+"   ");
						}
						else if(cd.Column_Type.toLowerCase().contains("date")){
							String S = "";
							for(int k = 0; k < 19; k++)
								S += (char)TableFile.readByte();
							System.out.print(S.replace("_00:00:00", "")+"   ");
						}
		            	
		            }
		            System.out.println();
		        }
				
    		}
    	}
	}


	public static void insert(String userCommand) {
    	String TableName = userCommand.split(" ")[2].trim().toLowerCase();
    	int indexOfOpenBracket = userCommand.indexOf("(");
    	int indexOfLastBracket = userCommand.lastIndexOf(")");
    	String[] Values = userCommand.substring(indexOfOpenBracket+1, indexOfLastBracket).trim().split(",");
    	if(!TableExists(TableName)){
    		System.out.println("Table does not Exist. Create Table First.");
    	}
    	else{
    		try {
    			//TreeMap tm = new TreeMap();
				RandomAccessFile TableFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+TableName+".tbl", "rw");
				RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema/information_schema.columns.tbl", "rw");
				long fileLength1 = TableFile.length();
			    TableFile.seek(fileLength1);
			    int offset = (int) TableFile.getFilePointer();
				int k=0;
				while(true){
					byte varcharLength = columnsTableFile.readByte();
					String TableSchema = "";
					for(int i = 0; i < varcharLength; i++)
						TableSchema += (char)columnsTableFile.readByte();
					byte varcharLength1 = columnsTableFile.readByte();
					String table = "";
					for(int i = 0; i < varcharLength1; i++)
						table += (char)columnsTableFile.readByte();
					byte varcharLength2 = columnsTableFile.readByte();
					String column = "";
					for(int i = 0; i < varcharLength2; i++)
						column += (char)columnsTableFile.readByte();
					int OP = columnsTableFile.readInt();
					byte varcharLength3 = columnsTableFile.readByte();
					String columnType = "";
					for(int i = 0; i < varcharLength3; i++)
						columnType += (char)columnsTableFile.readByte();
					byte varcharLength4 = columnsTableFile.readByte();
					String nullable = "";
					for(int i = 0; i < varcharLength4; i++)
						nullable += (char)columnsTableFile.readByte();
					byte varcharLength5 = columnsTableFile.readByte();
					String key = "";
					for(int i = 0; i < varcharLength5; i++)
						key += (char)columnsTableFile.readByte();
					if(TableName.equalsIgnoreCase(table)){
						if(columnType.toLowerCase().contains("byte")){
							
							TableFile.writeBytes(Values[k]);
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Values[k]))
							{
								ArrayList A = (ArrayList) tm.get(Values[k]);
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Short.parseShort(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("short");
							k++;
							//System.out.print("byte");
							k++;
						}
						else if(columnType.toLowerCase().contains("short")){
							TableFile.writeShort(Short.parseShort(Values[k]));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Short.parseShort(Values[k])))
							{
								ArrayList A = (ArrayList) tm.get(Short.parseShort(Values[k]));
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Short.parseShort(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("short");
							k++;
						}
						else if(columnType.toLowerCase().contains("int")){
							TableFile.writeInt(Integer.parseInt(Values[k]));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Integer.parseInt(Values[k])))
							{
								ArrayList A = (ArrayList) tm.get(Integer.parseInt(Values[k]));
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Integer.parseInt(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print(Integer.parseInt(Values[k]));
							k++;
						}
						else if(columnType.toLowerCase().contains("long")){
							TableFile.writeLong(Long.parseLong(Values[k]));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Long.parseLong(Values[k])))
							{
								ArrayList A = (ArrayList) tm.get(Long.parseLong(Values[k]));
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Long.parseLong(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("long");
							k++;
						}
						else if(columnType.toLowerCase().contains("char")){
							TableFile.writeByte(Values[k].replace("'","").length()); 
							TableFile.writeBytes(Values[k].replace("'",""));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Values[k].replace("'","")))
							{
								ArrayList A = (ArrayList) tm.get(Values[k].replace("'",""));
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Values[k].replace("'",""), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("char");
							k++;
						}
						else if(columnType.toLowerCase().contains("float")){
							TableFile.writeFloat(Float.parseFloat(Values[k]));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Float.parseFloat(Values[k])))
							{
								ArrayList A = (ArrayList) tm.get(Float.parseFloat(Values[k]));
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Float.parseFloat(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							k++;
						}
						else if(columnType.toLowerCase().contains("double")){
							TableFile.writeDouble(Double.parseDouble(Values[k]));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Double.parseDouble(Values[k])))
							{
								ArrayList A = (ArrayList) tm.get(Double.parseDouble(Values[k]));
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Double.parseDouble(Values[k]), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("double");
							k++;
						}
						else if(columnType.toLowerCase().contains("datetime")){
							TableFile.writeBytes(Values[k].replace("'",""));
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Values[k].replace("'","")))
							{
								ArrayList A = (ArrayList) tm.get(Values[k].replace("'",""));
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Values[k].replace("'",""), A );
							}
							putMap(TableName,column,columnType,tm);
							//System.out.print("datetime");
							k++;
						}
						else if(columnType.toLowerCase().contains("date")){
							//System.out.print("date");
							TableFile.writeBytes(Values[k].replace("'","")+"_00:00:00");
							TreeMap<Object, ArrayList> tm = getMap(TableName,column,columnType);
							if(tm.containsKey(Values[k].replace("'","")+"_00:00:00"))
							{
								ArrayList A = (ArrayList) tm.get(Values[k].replace("'","")+"_00:00:00");
								A.add(offset);
							}
							else
							{
								ArrayList A = new ArrayList();
								A.add(offset);
								tm.put(Values[k].replace("'","")+"_00:00:00", A );
							}
							putMap(TableName,column,columnType,tm);
							k++;
						}
					}
				}
				
			} catch (Exception e) {
			}
    		
    	}
	}

    public static TreeMap getMap(String tableName, String columnName,String columnType ) throws IOException {
    	 	TreeMap<Object, ArrayList> tm = new TreeMap();
    	 	RandomAccessFile IndexFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+tableName+"."+columnName+".ndx", "rw");
    	 		
    	 	if(IndexFile.length()!=0){
	    	 	if(columnType.toLowerCase().contains("byte")){
	    	 		try {
						while(true){
							Object Key;
						
								Key = IndexFile.readByte();
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("short")){
					try {
						while(true){
							Object Key;
						
								Key = IndexFile.readShort();
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("int")){
					try {
					while(true){
						Object Key;
					
							Key = IndexFile.readInt();
						
						int Count = IndexFile.readInt();
						ArrayList A = new ArrayList();
						for(int i=0;i<Count;i++){
							A.add(IndexFile.readInt());
						}
						tm.put(Key, A);
					}
					} catch (IOException e) {
					}
				}
				else if(columnType.toLowerCase().contains("long")){
					try {
						while(true){
							Object Key;
						
								Key = IndexFile.readLong();
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("char")){
					try {
						while(true){
							Object Key;
							byte varcharLength = IndexFile.readByte();
							String key_string = "";
							for(int i = 0; i < varcharLength; i++)
								key_string += (char)IndexFile.readByte();
							Key = key_string;
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("float")){
					try {
						while(true){
							Object Key;
						
								Key = IndexFile.readFloat();
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("double")){
					try {
						while(true){
							Object Key;
						
								Key = IndexFile.readDouble();
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("datetime")){
					try {
						while(true){
							Object Key;
							int varcharLength = 19;
							String key_string = "";
							for(int i = 0; i < varcharLength; i++)
								key_string += (char)IndexFile.readByte();
							Key = key_string;
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
				else if(columnType.toLowerCase().contains("date")){
					try {
						while(true){
							Object Key;
							int varcharLength = 19;
							String key_string = "";
							for(int i = 0; i < varcharLength; i++)
								key_string += (char)IndexFile.readByte();
							Key = key_string;
							
							int Count = IndexFile.readInt();
							ArrayList A = new ArrayList();
							for(int i=0;i<Count;i++){
								A.add(IndexFile.readInt());
							}
							tm.put(Key, A);
						}
						} catch (IOException e) {
						}
				}
    	 	}
    	return tm;
    }
    
    public static void putMap(String tableName, String columnName,String columnType, TreeMap<Object, ArrayList> tm  ) throws IOException{
	 	RandomAccessFile IndexFile = new RandomAccessFile(ActiveSchema+"/"+ActiveSchema+"."+tableName+"."+columnName+".ndx", "rw");
	 	
	 	if(columnType.toLowerCase().contains("byte")){

			   Set keys = tm.keySet();
		
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Byte key = (Byte) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeByte(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("short")){
			   Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Short key = (Short) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeShort(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("int")){
			
			   Set keys = tm.keySet();
		
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Integer key = (Integer) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeInt(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("long")){
			   Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Long key = (Long) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeLong(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("char")){
			   Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     String key = (String) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeByte(key.length());
			     IndexFile.writeBytes(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("float")){
			 Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Float key = (Float) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeFloat(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("double")){
			 Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     Double key = (Double) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeDouble(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("datetime")){
			  Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     String key = (String) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeBytes(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}
		else if(columnType.toLowerCase().contains("date")){
			 Set keys = tm.keySet();
				
			   for (Iterator i = keys.iterator(); i.hasNext();) {
			     String key = (String) i.next();
			     ArrayList value = (ArrayList) tm.get(key);
			     //System.out.println(key + " = " + value);
			     IndexFile.writeBytes(key);
			     IndexFile.writeInt(value.size());
			     Iterator iter = value.iterator();
			     for(int j=0; j < value.size();j++) {
			    	  IndexFile.writeInt((int) value.get(j));
			    	  
			      }
			   }
		}	
    	
	
}

	public static boolean TableExists(String tableName) {
			
			try{
				
				RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema/information_schema.table.tbl", "rw");
				while(true){
					byte varcharLength = tablesTableFile.readByte();
					String ActiveSchema = "";
					for(int i = 0; i < varcharLength; i++)
						ActiveSchema += (char)tablesTableFile.readByte();
					byte varcharLength1 = tablesTableFile.readByte();
					String table = "";
					for(int i = 0; i < varcharLength1; i++)
						table += (char)tablesTableFile.readByte();
					long a = tablesTableFile.readLong();
					
					if(tableName.equalsIgnoreCase(table)){
						return true;
					}
				}
				
			}
			catch(Exception e){
				return false;
			}
			
		
	}


	public static void createTable(String userCommand) {
    	String TableName = userCommand.split(" ")[2];
    	if(!TableExists(TableName)){
	    	int indexOfOpenBracket = userCommand.indexOf("(");
	    	int indexOfLastBracket = userCommand.lastIndexOf(")");
	    	String[] Columns = userCommand.substring(indexOfOpenBracket+1, indexOfLastBracket).trim().split(",");
	    	try {
	    		
	    		RandomAccessFile tablesTableFile = new RandomAccessFile("information_schema/information_schema.table.tbl", "rw");
				RandomAccessFile columnsTableFile = new RandomAccessFile("information_schema/information_schema.columns.tbl", "rw");
				long fileLength = tablesTableFile.length();
				tablesTableFile.seek(fileLength);
				long fileLength1 = columnsTableFile.length();
				columnsTableFile.seek(fileLength1);
		    	tablesTableFile.writeByte(ActiveSchema.length()); // TABLE_SCHEMA
				tablesTableFile.writeBytes(ActiveSchema);
				tablesTableFile.writeByte(TableName.length()); // TABLE_NAME
				tablesTableFile.writeBytes(TableName);
				tablesTableFile.writeLong(0);
				tablesTableFile.close();
		
	    	
			for(int i=0;i<Columns.length;i++){
				boolean PK = false;
				boolean notnull = false;
				String column = Columns[i].trim();
				if(column.contains("primary key"))
				{
					column = column.replace("primary key","").trim();
					PK = true;
				}
				if(column.contains("not null"))
				{
					column = column.replace("not null","").trim();
					notnull = true;
				}
				String columnName = column.split(" ", 2)[0].trim();
				String colunmType = column.split(" ", 2)[1].trim();
				columnsTableFile.writeByte(ActiveSchema.length()); // TABLE_SCHEMA
				columnsTableFile.writeBytes(ActiveSchema);
				columnsTableFile.writeByte(TableName.length()); // TABLE_NAME
				columnsTableFile.writeBytes(TableName);
				columnsTableFile.writeByte(columnName.length()); // COLUMN_NAME
				columnsTableFile.writeBytes(columnName);
				columnsTableFile.writeInt(i+1); // ORDINAL_POSITION
				columnsTableFile.writeByte(colunmType.length()); // COLUMN_TYPE
				columnsTableFile.writeBytes(colunmType);
				if(PK)
				{
					columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
					columnsTableFile.writeBytes("NO");
					columnsTableFile.writeByte("PRI".length()); // COLUMN_KEY
					columnsTableFile.writeBytes("PRI");
				}
				else{
					if(notnull){
						columnsTableFile.writeByte("NO".length()); // IS_NULLABLE
						columnsTableFile.writeBytes("NO");
					}
					else
					{
						columnsTableFile.writeByte("YES".length()); // IS_NULLABLE
						columnsTableFile.writeBytes("YES");	
					}
					columnsTableFile.writeByte("".length()); // COLUMN_KEY
					columnsTableFile.writeBytes("");
				}
				
				
				
					
			}
			columnsTableFile.close();
	    	} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	else{
    		System.out.println("Table Already Exists");
    	}
	}


	public static void showSchemas() {
    	try {
	    	
	    	RandomAccessFile schemataTableFile = new RandomAccessFile("information_schema/information_schema.schemata.tbl", "rw");
	    	try{
		    	while(true)
		    	{
		    		byte varcharLength = schemataTableFile.readByte();
					for(int i = 0; i < varcharLength; i++)
						System.out.print((char)schemataTableFile.readByte());
					System.out.println();
		    	}
	    	}
	    	catch(EOFException ex){
	    		
	    	}
	    	
			schemataTableFile.close();
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
    
    public static void useSchema(String schemaName) {
    	try {
    		
    		ActiveSchema = schemaName;
    		System.out.println("Current Schema is " + ActiveSchema);
    		
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	public static void createSchema(String schemaName) {
    	try {
	    	new File(schemaName).mkdir();
	    	ActiveSchema = schemaName;
	    	RandomAccessFile schemataTableFile = new RandomAccessFile("information_schema/information_schema.schemata.tbl", "rw");
			long fileLength = schemataTableFile.length();
			schemataTableFile.seek(fileLength);
			schemataTableFile.writeByte(schemaName.length());
			schemataTableFile.writeBytes(schemaName);
			schemataTableFile.close();
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",80));
		System.out.println();
		System.out.println("\tshow schemas;      	Display all schemas defined in your database.");
		System.out.println("\tuse;                	Chooses a Schema.");
		System.out.println("\tshow tables;   		Display all records in the table.");
		System.out.println("\tcreate schema; 		Creates a new schema to hold tables.");
		System.out.println("\tcreate table; 		Creates a new table.");
		System.out.println("\tinsert into table;	Inserts a row or record into a table.");
		System.out.println("\tdrop table;        	Remove a table scema and all of its contained data.");
		System.out.println("\tselect-from-where;	Style Query.");
		System.out.println("\tversion;       		Show the program version.");
		System.out.println("\thelp;          		Show this help information");
		System.out.println("\texit;          		Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	
	/**
	 *  Display the welcome "splash screen"
	 */
	public static void splashScreen() {
		System.out.println(line("*",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("*",80));
	}

	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/**
	 * @param num The number of newlines to be displayed to <b>stdout</b>
	 */
	public static void newline(int num) {
		for(int i=0;i<num;i++) {
			System.out.println();
		}
	}
	
	public static void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}


	/**
	 *  This method reads a binary table file using a hard-coded table schema.
	 *  Your query must be able to read a binary table file using a dynamically 
	 *  constructed table schema from the information_schema
	 */
	public static void displayAllRecords() {
		try {
			/* Open the widget table binary data file */
			RandomAccessFile widgetTableFile = new RandomAccessFile(widgetTableFileName, "rw");

			/*
			 *  Navigate throught the binary data file, displaying each widget record
			 *  in the order that it physically appears in the file. Convert binary data
			 *  to appropriate data types for each field.
			 */
			for(int record = 0;record < 5; record++) {
				System.out.print(widgetTableFile.readInt());
				System.out.print("\t");
				byte varcharLength = widgetTableFile.readByte();
				for(int i = 0; i < varcharLength; i++)
					System.out.print((char)widgetTableFile.readByte());
				System.out.print("\t");
				System.out.print(widgetTableFile.readShort());
				System.out.print("\t");
				System.out.println(widgetTableFile.readFloat());
			}
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

	public static void displayRecordID(int id) {
		try {
			int indexFileLocation = 0;
			long indexOfRecord = 0;
			boolean recordFound = false;

			RandomAccessFile widgetTableFile = new RandomAccessFile(widgetTableFileName, "rw");
			RandomAccessFile tableIdIndex = new RandomAccessFile(tableIdIndexName, "rw");

			/*
			 *  Use exhaustive brute force seach over the binary index file to locate
			 *  the requested ID values. Then use its assoicated address to seek the 
			 *  record in the widget table binary data file.
			 *
			 *  You may instead want to load the binary index file into a HashMap
			 *  or similar key:value data structure for efficient index-address lookup,
			 *  but this is not required.
			 */
			while(!recordFound) {
				tableIdIndex.seek(indexFileLocation);
				if(tableIdIndex.readInt() == id) {
					tableIdIndex.seek(indexFileLocation+4);
					indexOfRecord = tableIdIndex.readLong();
					recordFound = true;
				}
				/* 
				 *  Each index entry uses 12 bytes: ID=4-bytes + address=8-bytes
				 *  Move ahead 12 bytes in the index file for each while() loop
				 *  iteration to increment through index entries.
				 * 
				 */
				indexFileLocation += 12;
			}

			widgetTableFile.seek(indexOfRecord);
			System.out.print(widgetTableFile.readInt());
			System.out.print("\t");
			byte varcharLength = widgetTableFile.readByte();
			for(int i = 0; i < varcharLength; i++)
				System.out.print((char)widgetTableFile.readByte());
			System.out.print("\t");
			System.out.print(widgetTableFile.readShort());
			System.out.print("\t");
			System.out.println(widgetTableFile.readFloat());
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

	/**
	 *  This method is hard-coded to create a binary table file with 5 records
	 *  It also creates an index file for the ID field
	 *  It is based on the following table schema:
	 *  
	 *  CREATE TABLE table (
	 *      id unsigned int primary key,
	 *      name varchar(25),
	 *      quantity unsigned short,
	 *      probability float
	 *  );
	 */
	public static void hardCodedCreateTableWithIndex() {
		long recordPointer;
		try {
			RandomAccessFile widgetTableFile = new RandomAccessFile(widgetTableFileName, "rw");
			RandomAccessFile tableIdIndex = new RandomAccessFile(tableIdIndexName, "rw");
			
			id = 1;
			name = "alpha";
			quantity = 847;
			probability = 0.341f;
			
			tableIdIndex.writeInt(id);
			tableIdIndex.writeLong(widgetTableFile.getFilePointer());
			widgetTableFile.writeInt(id);
			widgetTableFile.writeByte(name.length());
			widgetTableFile.writeBytes(name);
			widgetTableFile.writeShort(quantity);
			widgetTableFile.writeFloat(probability);
			
			id = 2;
			name = "beta";
			quantity = 1472;
			probability = 0.89f;
			
			tableIdIndex.writeInt(id);
			tableIdIndex.writeLong(widgetTableFile.getFilePointer());
			widgetTableFile.writeInt(id);
			widgetTableFile.writeByte(name.length());
			widgetTableFile.writeBytes(name);
			widgetTableFile.writeShort(quantity);
			widgetTableFile.writeFloat(probability);

			id = 3;
			name = "gamma";
			quantity = 41;
			probability = 0.5f;
			
			tableIdIndex.writeInt(id);
			tableIdIndex.writeLong(widgetTableFile.getFilePointer());
			widgetTableFile.writeInt(id);
			widgetTableFile.writeByte(name.length());
			widgetTableFile.writeBytes(name);
			widgetTableFile.writeShort(quantity);
			widgetTableFile.writeFloat(probability);

			id = 4;
			name = "delta";
			quantity = 4911;
			probability = 0.4142f;
			
			tableIdIndex.writeInt(id);
			tableIdIndex.writeLong(widgetTableFile.getFilePointer());
			widgetTableFile.writeInt(id);
			widgetTableFile.writeByte(name.length());
			widgetTableFile.writeBytes(name);
			widgetTableFile.writeShort(quantity);
			widgetTableFile.writeFloat(probability);

			id = 5;
			name = "epsilon";
			quantity = 6823;
			probability = 0.618f;
			
			tableIdIndex.writeInt(id);
			tableIdIndex.writeLong(widgetTableFile.getFilePointer());
			widgetTableFile.writeInt(id);
			widgetTableFile.writeByte(name.length());
			widgetTableFile.writeBytes(name);
			widgetTableFile.writeShort(quantity);
			widgetTableFile.writeFloat(probability);
		}
		catch(Exception e) {
			System.out.println(e);
		}
	}

}
class ColumnDetails
{
	String Column_Name;
	int Ordinal_Position;
	String Column_Type;
	String IsNullable;
	String Key;
}
