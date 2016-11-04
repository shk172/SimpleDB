package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    //These are the variables that I implemented:
    int tableid;
    TransactionId tid;
    String tableAlias;
    DbFile file; //Database file we'll need to pull the tables from
    Catalog catalog; //Catalog to pull the table information from

    /////
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        //Get the database file we'll need to use for later from the catalog
        catalog = Database.getCatalog(); 
        file = catalog.getDatabaseFile(tableid);
        
    }
    
    //This was already implemented in the skeleton code
    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }
    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	//look up the name from the catalog with the table id
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
    	//As the constructor states, if alias is null, return the string "null"
    	if(tableAlias.equals(null))
    		return "null";
    	else
    		return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        tableid = (Integer) null;
        tableAlias = null;
    }





    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = file.getTupleDesc();
        
        //make a new TupleDesc with field names that have alias as prefix
        Type[] typeArray = new Type[tupleDesc.numFields()];
        String[] fieldArray = new String[tupleDesc.numFields()];
       
        for(int i = 0; i < tupleDesc.numFields(); i++){
        	typeArray[i] = tupleDesc.getFieldType(i);
        	
        	//Use StringBuilder to combine tableAlias and fieldName together
        	StringBuilder fieldNameBuilder = new StringBuilder();
        	fieldNameBuilder.append(tableAlias);
        	fieldNameBuilder.append(tupleDesc.getFieldName(i));
        	
        	//Then pass the new string to the field array
        	fieldArray[i] = fieldNameBuilder.toString();
        }
        TupleDesc newTupleDesc = new TupleDesc(typeArray, fieldArray);
        return newTupleDesc;
    }
    
    boolean open = false;
    ArrayList<Tuple> tuples;
	int i;
	int p;
    
    public void open() throws DbException, TransactionAbortedException {
    	loadNewPage(p);
        open = true;
    }
    
    public boolean hasNext() throws TransactionAbortedException, DbException {
		if (!open) {
			return false;
		}
		if (i < tuples.size()) {
			return true;
		}
		if (!loadNewPage(++p)) {
			return false;
		}
		while(tuples.size() == 0) {
			boolean loaded = loadNewPage(++p);
			if (!loaded) {
				return false;
			}
		}
		return true;
	}

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
		if (!open) {
			throw new NoSuchElementException();
		}
		
		return tuples.get(i++);

	}
	

    public void close() {
        open = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
		p = 0;
		i = 0;
		loadNewPage(p);
	}
    
    private boolean loadNewPage(int pgNum) throws DbException, TransactionAbortedException {
		HeapPageId pid = new HeapPageId(tableid, p);
		//this will succeed because its in buffer
		try {
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
			//System.out.println("number of tuples in file: " + (page.numSlots - page.getNumEmptySlots()));
			tuples = new ArrayList<Tuple>();
			Iterator<Tuple> pi = page.iterator();
			
			while (pi.hasNext()) {
				tuples.add(pi.next());
			}
			//System.out.println("Size of tuples: " + tuples.size());
			i = 0;
			return true;
		} catch (DbException e) {
			return false;
		}
	}
}

