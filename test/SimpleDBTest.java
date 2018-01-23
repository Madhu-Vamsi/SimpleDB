package test;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

public class SimpleDBTest {

	public static void main(String[] args) {
		SimpleDB.init("simpleDB");
		
		System.out.println("Start LRU(k) testing"); 
		Block blk[] = new Block[8];
		for(int i=0;i<8;i++){
			blk[i] = new Block("temp", i);
		}
		Buffer buff[] = new Buffer[8];
		BufferMgr basicBuffMgr = new SimpleDB().bufferMgr();
		
		basicBuffMgr.flushAll(1);
		try {
			for(int i=0;i<8;i++) {
				buff[i] = basicBuffMgr.pin(blk[i]);
			}
			basicBuffMgr.pin(blk[3]);
			basicBuffMgr.pin(blk[1]);
			basicBuffMgr.pin(blk[6]);
			basicBuffMgr.pin(blk[0]);
			basicBuffMgr.unpin(buff[7]);
			basicBuffMgr.unpin(buff[6]);
			basicBuffMgr.unpin(buff[5]);
			basicBuffMgr.unpin(buff[4]);
			basicBuffMgr.unpin(buff[3]);
			basicBuffMgr.unpin(buff[0]);
			basicBuffMgr.unpin(buff[6]);
			basicBuffMgr.unpin(buff[3]);
			basicBuffMgr.unpin(buff[1]);
			basicBuffMgr.unpin(buff[1]);
			basicBuffMgr.pin(new Block("temp", 8));
			
		}catch (BufferAbortException e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}

	}

}
