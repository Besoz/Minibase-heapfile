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
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class Scan {
	boolean open;// y 
	Heapfile heapFile;
	
	HFPage curPage;
	RID    curRID;
	public Scan(Heapfile heapFile) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException
	{
		this.heapFile=heapFile;
		curPage =new HFPage( heapFile.getHFPage());
		curRID=curPage.firstRecord();
		SystemDefs.JavabaseBM.pinPage(curPage.getCurPage(), curPage, false);
		
	
	}
	public Tuple getNext(RID rid) throws InvalidPageNumberException, FileIOException, IOException, InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
//		rid output the RID of the next page
//		out tuple of the next 
//		 
		Tuple t= null;
		if(curRID!=null)
		{
			rid.slotNo=curRID.slotNo;
			rid.pageNo=curRID.pageNo;
			t=curPage.getRecord(curRID);		
			curRID=curPage.nextRecord(curRID);
		}
		if (curRID==null){			
			SystemDefs.JavabaseBM.unpinPage(curPage.getCurPage(), true);
			if(curPage.getNextPage().pid!=-1)
			{
				SystemDefs.JavabaseBM.pinPage(curPage.getNextPage(), curPage, false);
				curRID=curPage.firstRecord();
				
			}
			else 
			{
				SystemDefs.JavabaseBM.pinPage(curPage.getCurPage(), curPage, false);					
			}
			
		}
		if(t==null)
			SystemDefs.JavabaseBM.unpinPage(curPage.getCurPage(), true);
		return t;	
	}
	
	public boolean position(RID rid) throws ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, IOException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException{
		
//		curRID=rid;
//		pin page rid
//		loop page on page 
		if (rid==null){
			return false;
		}
		if(curRID.pageNo != rid.pageNo){
			SystemDefs.JavabaseBM.unpinPage(curPage.getCurPage(), true);
			curRID=rid;
			SystemDefs.JavabaseBM.pinPage(curRID.pageNo, curPage, false);
		}
		else{
			curRID=rid;
		}
		
		return true;
	}
	public void closescan() {
			open = false;
			heapFile = null;
			try{
			SystemDefs.JavabaseBM.unpinPage(curPage.getCurPage(), true);
			}catch(Exception e){
				
			}

	}


}
