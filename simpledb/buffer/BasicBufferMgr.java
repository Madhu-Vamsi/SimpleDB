package simpledb.buffer;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.swing.text.html.HTMLDocument.HTMLReader.BlockAction;

import simpledb.file.Block;
import simpledb.file.FileMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private Map<Block, Buffer> bufferPoolMap;
	private Map<Block, Deque<Long>> timeStampMap;
	private Set<Block> zeroPinnedBlocks;
	private static int lruK = 2;

	/**
	 * Creates a buffer manager having the specified number of buffer slots. This
	 * constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer();
		bufferPoolMap = new HashMap<Block, Buffer>();
		timeStampMap = new HashMap<Block, Deque<Long>>();
		zeroPinnedBlocks = new HashSet<Block>();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer assigned
	 * to that block then that buffer is used; otherwise, an unpinned buffer from
	 * the pool is chosen. Returns a null value if there are no available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			System.out.println("Block not found in bufferPoolMap");
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			if(buff.block() != null)bufferPoolMap.remove(buff.block());
			if(buff.block() != null)timeStampMap.remove(buff.block());
			System.out.println("Allocated Buffer : "+buff.toString()+" to Block : "+blk.number());
			buff.assignToBlock(blk);
			bufferPoolMap.put(blk, buff);
			Deque<Long> timestamps = new LinkedList<Long>();
			timestamps.addLast(System.currentTimeMillis());
			timeStampMap.put(blk, timestamps);
		} else {
			Deque timestamps = timeStampMap.get(blk);
			if (timestamps.size() > lruK) {
				timestamps.removeFirst();
			}
			timestamps.addLast(System.currentTimeMillis());
		}
		/*
		 * if (!buff.isPinned()) numAvailable--;
		 */
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it. Returns
	 * null (without allocating the block) if there are no available buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned()) {
			// numAvailable++;
			zeroPinnedBlocks.add(buff.block());
		}

	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		/*
		 * for (Buffer buff : bufferpool) { Block b = buff.block(); if (b != null &&
		 * b.equals(blk)) return buff; }
		 */
		if (bufferPoolMap.containsKey(blk))
			return bufferPoolMap.get(blk);
		return null;
	}

	private Buffer chooseUnpinnedBuffer() {
		
		//for (Buffer buff : bufferpool) if (!buff.isPinned()) return buff;
		
		long curTime = System.currentTimeMillis();
		if (numAvailable > 0){
			numAvailable -= 1;
			return bufferpool[numAvailable];
		}
		if (zeroPinnedBlocks.size() <= 0)
			return null;
		long curMax = -1;
		Deque<Long> temp;
		long kthTime = 0;
		Block flaggedblk = null;
		for (Block blk : zeroPinnedBlocks) {
			temp = timeStampMap.get(blk);
			if (temp.size() < lruK) {
				curMax = Long.MAX_VALUE;
				break;
			} else {
				kthTime = temp.peekFirst();
				if (curMax < kthTime) {
					curMax = kthTime;
					flaggedblk = blk;
				}
			}
		}
		if (curMax == Long.MAX_VALUE) {
			long oldTimeStamp = Long.MAX_VALUE;
			for (Block blk : zeroPinnedBlocks) {
				temp = timeStampMap.get(blk);
				if (temp.size() < lruK && temp.peekLast() < oldTimeStamp) {
					oldTimeStamp = temp.peekLast();
					flaggedblk = blk;
				}
			}
			System.out.println("Removed Block number : "+flaggedblk.number());
			return bufferPoolMap.get(flaggedblk);
		} else{
			System.out.println("Removed Block number : "+flaggedblk.number());
			return bufferPoolMap.get(flaggedblk);
		}
	}
}
