package heap;

import java.io.IOException;

import bufmgr.BufMgr;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class Heapfile implements GlobalConst {
	private HFPage hfPage;
	private int recCount;
	String fileName;

	public Heapfile(String string) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException
	{
		this.fileName=string;
		if(string !=null)
		{
			hfPage = new HFPage();
			if(SystemDefs.JavabaseDB.get_file_entry(string)==null)
			{
				Page p = new Page();
				PageId pid=SystemDefs.JavabaseBM.newPage(p,1);
				hfPage.init(pid, p);
				SystemDefs.JavabaseDB.add_file_entry(string, pid);
				SystemDefs.JavabaseBM.unpinPage(pid, true);
				recCount=0;
			}else
			{
				PageId pid = SystemDefs.JavabaseDB.get_file_entry(string);
			    SystemDefs.JavabaseBM.pinPage(pid, hfPage, false);
				SystemDefs.JavabaseBM.unpinPage(pid, true);
				recCount =countRecords();
			}			
			
		}
		
	}
	
	

	public RID insertRecord(byte[] byteArray) throws ChainException, IOException{
		HFPage temp = hfPage;
		if(byteArray.length>HFPage.MAX_SPACE)
		{
			throw new SpaceNotAvailableException(new Exception(), "Not Enough Space");
		}
		while(temp.getCurPage().pid!=-1)
		{
			if(temp.available_space()>=byteArray.length)
			{
				recCount++;
				RID rid =  temp.insertRecord(byteArray);
				return rid;
			}
			else if(temp.getNextPage().pid!=-1) {
				HFPage temp2 = new HFPage();
				HFPage apage = new HFPage();
				SystemDefs.JavabaseBM.pinPage(temp.getNextPage(), apage, false);
				temp2.setCurPage(temp.getNextPage());
				temp2.openHFpage(apage);				
				temp2.setPrevPage(temp.getCurPage());
				temp2.setCurPage(temp.getNextPage());
				temp2.setNextPage(apage.getNextPage());
				SystemDefs.JavabaseBM.unpinPage(temp.getNextPage(),true);
				temp = temp2;
				
			}
			else {
				Page apage = new Page();
				PageId pid = SystemDefs.JavabaseBM.newPage(apage, 1);
				HFPage newHF = new HFPage();
				newHF.init(pid, apage);
				newHF.setNextPage(new PageId(-1));
				newHF.setPrevPage(temp.getCurPage());
				temp.setNextPage(pid);
				temp = newHF;
				SystemDefs.JavabaseBM.unpinPage(pid, true);
				recCount++;
				RID rid =  temp.insertRecord(byteArray);
				return rid;
				}
		}
		return null;
		
		
	}


	public int getRecCnt() {
		
		return recCount;
	}

	public Scan openScan() throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException {
		Scan scan = new Scan(this);
		return scan;
	}


	public boolean deleteRecord(RID rid) throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException
	{
		PageId pid = new PageId(rid.pageNo.pid);
		HFPage page = new HFPage();
		SystemDefs.JavabaseBM.pinPage(pid, page, false);
		try{
			page.deleteRecord(rid);
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			recCount--;
			return true;
		}catch(Exception e)
		{
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			return false;
		}
		
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException, IOException{
		PageId pid = new PageId(rid.pageNo.pid);
		HFPage page = new HFPage();
		SystemDefs.JavabaseBM.pinPage(pid, page, false);
		SystemDefs.JavabaseBM.unpinPage(pid, true);

		
				Tuple t = page.returnRecord(rid);
				if(newTuple.getLength()!=t.getLength())
				{
					throw new InvalidUpdateException(new Exception(), "Cannot Shrink Record");
					
				}
				t.tupleCopy(newTuple);
			
			
			return true;
		
	}

	public Tuple getRecord(RID rid) throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException{
		PageId pid = new PageId(rid.pageNo.pid);
		HFPage page = new HFPage();
		SystemDefs.JavabaseBM.pinPage(pid, page, false);
		try{
			Tuple t = page.getRecord(rid);
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			return t;
		}catch(Exception e)
		{
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			return null;
		}
	}


	 public void deleteFile() throws FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException, IOException {
			SystemDefs.JavabaseDB.delete_file_entry(this.fileName);
	 }
	private int countRecords() throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException
	 {
	  int counter=0;
	  HFPage temp = hfPage;
	  while(temp.getCurPage().pid!=-1)
	  {
	   RID rid = temp.firstRecord();
	   while(rid!=null)
	   {
	    counter++;
	    rid = temp.nextRecord(rid);    
	   }
	   HFPage temp2 = new HFPage();
	   if(temp.getNextPage().pid!=-1)
	   {
	    SystemDefs.JavabaseBM.pinPage(temp.getNextPage(), temp2, false);
	    SystemDefs.JavabaseBM.unpinPage(temp.getNextPage(),false);
	   }
	   else temp2.setCurPage(new PageId(-1));
	   

	   temp = temp2;
	   
	  }
	  
	  return counter;
	 }

	public HFPage getHFPage() {
	
		return hfPage;
	}
	


}
