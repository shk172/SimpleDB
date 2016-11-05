package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	TupleDesc td;
	File file;
	int id;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.td = td;
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	//First, we import the buffer pool to get the pages' information
    	Database.getBufferPool();
    	int totalPages = ((int) file.length()) / BufferPool.getPageSize();
    	byte[] page = new byte[BufferPool.getPageSize()];
    	
    	
    	//If the pid's page number exceeds the pages in the file return exception
    	if (pid.pageNumber() >= totalPages) {
    		throw new IllegalArgumentException("table number too high");
    	}
    	
    	//Try to read the page from the file with fileinputstream and return it in a HeapPage
    	// If the file is not found or page number is out of bounds, throw exception
    	try {
    		FileInputStream fis = new FileInputStream(file);
    		fis.skip(pid.pageNumber() * BufferPool.getPageSize());
			fis.read(page);
			fis.close();
			return new HeapPage((HeapPageId) pid, page);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("file not found");
		} catch (IOException i) {
			throw new IllegalArgumentException("page number out of bounds");
		}
		
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return ((int) file.length()) / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	ArrayList<Tuple> tuples;
    	TransactionId tid;
    	HeapFile hf;
    	int p;
    	boolean open = false;
    	int tableId;
    	Iterator<Tuple> tupleIterator;
    	
    	//Constructor
    	public HeapFileIterator(TransactionId tid, HeapFile hf) {
    		this.tid = tid;
    		this.hf = hf;
    		this.tableId = hf.getId();
    		p = 0;
    	}
    	
		@Override
	    public void open() throws DbException,
        TransactionAbortedException {
			open = true;
			tupleIterator = loadPageTuples();
		}

		//this method will be called each time we need to call a new page
		//It loads a new heap page and returns its iterator so that we can iterate through its tuples.
		private Iterator<Tuple> loadPageTuples() throws DbException,
		                            TransactionAbortedException{
			HeapPageId pid = new HeapPageId(tableId, p);
			HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			return p.iterator();
		}
		
	    public boolean hasNext() throws DbException,
        TransactionAbortedException {
	    	//If not open, return false
	    	if (open == false) 
	    		return false;
	    	
	    	//If the page has more tuples, return true
	    	else if (tupleIterator.hasNext()) {
	    		return true;
	    	} 
	    	
	    	//Otherwise, go to the next page, and read in new tuples
	    	else {
	    		p++;
	    		if (p < hf.numPages()) {
	    			tupleIterator = loadPageTuples();
	    			return tupleIterator.hasNext();
	    		} 
	    		else{
	    			return false;
	    		}
	    			
	    	}
	    }

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
	        if (hasNext())
	            return tupleIterator.next();
	        else
	            throw new NoSuchElementException();
	    }

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// reset iterator variables and load first page
			p = 0;
			tupleIterator = loadPageTuples();
		}

		@Override
		public void close() {
			open = false;
		}
    	
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

