package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
	
	int tableID;
	int pageNum;
	
    public HeapPageId(int tableId, int pgNo) {
    	this.tableID = tableId;
    	this.pageNum = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
    	return tableID;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
    	return pageNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	//As the description says, the hash code is literally concatenation of
    	// the tableID and pageNum, combined by adding the two as strings and 
    	// converting back to int.
    	String stringHash = Integer.toString(tableID) + Integer.toString(pageNum);
    	int integerHash = Integer.parseInt(stringHash);
    	return integerHash;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if (!(o instanceof PageId)) {
        	return false;
        }
        if (tableID != ((PageId) o).getTableId()) {
        	return false;
        }
        if(pageNum != ((PageId) o).pageNumber()){
        	return false;
        }
        return true;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
