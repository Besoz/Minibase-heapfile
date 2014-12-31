package heap;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;

public class Driver {
	public static void main(String[] args) throws BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException, DiskMgrException, IOException {
			String dbpath = "C:\\Users\\M.Elkholy\\Desktop\\minibase-db"; 
		    String logpath = "C:\\Users\\M.Elkholy\\Desktop\\minibase-log"; 
		    HFPage page1 = new HFPage();
		    SystemDefs sysdef = new SystemDefs(dbpath,100,100,"Clock");
		    PageId pid = SystemDefs.JavabaseBM.newPage(page1, 1);
		    SystemDefs.JavabaseBM.unpinPage(pid, true);


		    page1.init(pid, page1);
		    HFPage page2 = new HFPage();
		    PageId pid2 = SystemDefs.JavabaseBM.newPage(page2, 1);
		    SystemDefs.JavabaseBM.unpinPage(pid2, true);
		    page1.setNextPage(pid2);
		    page2.init(pid2, page2);
		    page2.setPrevPage(pid);
		    SystemDefs.JavabaseBM.pinPage(pid, page1, false);
		    SystemDefs.JavabaseBM.pinPage(pid2, page2, false);
		 
		    HFPage page3 = new HFPage();
		    SystemDefs.JavabaseBM.pinPage(pid2,page3,false);
		    
		    System.out.println(page3.getCurPage());
		    System.out.println(page3.getPrevPage());
		    System.out.println(page3.getNextPage());
	}
}
