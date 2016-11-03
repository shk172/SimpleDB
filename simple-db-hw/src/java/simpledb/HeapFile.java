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
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
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
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	
    	Database.getBufferPool();
    	
    	int totalPages = ((int) file.length()) / BufferPool.getPageSize();
    	if (pid.pageNumber() >= totalPages) {
    		throw new IllegalArgumentException("table number too high");
    	}
    	int page_num = pid.pageNumber();
    	byte page[] = new byte[BufferPool.getPageSize()];
    	try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.read(page);
			raf.close();
			return new HeapPage((HeapPageId) pid, page);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("file not found");
		} catch (IOException i) {
			throw new IllegalArgumentException("page " + page_num + " not found");
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
        Database.getBufferPool();
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
    	int i;
    	int p;
    	boolean open = false;
    	
    	public HeapFileIterator(TransactionId tid, HeapFile hf) {
    		this.tid = tid;
    		this.hf = hf;
    	}
    	
		private boolean load_new_page(int pgNum) throws DbException, TransactionAbortedException {
			HeapPageId pid = new HeapPageId(hf.getId(), p++);
			try {
				HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
				tuples = new ArrayList<Tuple>();
				
				Iterator<Tuple> pi = p.iterator();
				while (pi.hasNext()) {
					tuples.add(pi.next());
				}
				i = 0;
				return true;
			} catch (DbException e) {
				return false;
			}
		}
		@Override
		public void open() throws DbException, TransactionAbortedException {
			i = 0;
			p = 0;
			load_new_page(p);
			open = true;
		}
		
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (!open) {
				return false;
			}
			if (i < tuples.size()) {
				return true;
			}
			if (!load_new_page(p)) {
				System.out.print("out of pages!!!!!!!!");
				return false;
			}
			while(tuples.size() == 0) {
				boolean loaded = load_new_page(p);
				if (!loaded) {
					return false;
				}
			}
			return true;
		}


		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!open) {
				throw new NoSuchElementException();
			}

			return tuples.get(i++);

		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			p = 0;
			i = 0;
			load_new_page(p);
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

