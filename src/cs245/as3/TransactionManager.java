package cs245.as3;

import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

import javax.xml.bind.annotation.XmlInlineBinaryData;

/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private HashMap<Long, ArrayList<LogRecords>> transactionsets;

	private LogManager logManager;

	private StorageManager storageManager;

	private LogUtils util;

	public final static int maxLength = 128; // 单条日志最大长度

	public final static int begin = 16; // 日志起始位置

	private PriorityQueue<Long> pq;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
		util = new LogUtils();
		pq = new PriorityQueue<>();
		transactionsets = new HashMap<>();
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		this.storageManager = sm;
		this.logManager = lm;
		latestValues = sm.readStoredTable();

		ArrayList<LogRecords> logRecordsList = new ArrayList<>();

		HashSet<Long> commitIdx = new HashSet<>();
		// 从日志当前偏移量到日志结尾，磁盘中读取数据
		ArrayList<Integer> commitIdxList = new ArrayList<>();
		for (int idx = lm.getLogTruncationOffset(); idx < lm.getLogEndOffset();) {
			// 读取当前位置字节数组，每次读取16字节
			byte[] bytes = lm.readLogRecord(idx, begin);
			int size = util.getLogSize(bytes);
			ByteBuffer buffer = ByteBuffer.allocate(size);
			// 内存添加数据
			for (int i = 0; i < size; i += maxLength) {
				int l = Math.min(size - i, maxLength);
				buffer.put(lm.readLogRecord(idx + i, l));
			}
			byte[] recordBytes = buffer.array();
			// 字节数组转日志记录
			LogRecords record = LogRecords.changeToLogRecord(recordBytes);
			logRecordsList.add(record);
			// 提交日志
			if (record.getType() == 2) {
				commitIdx.add(record.getTxID());
			}
			idx += record.size();
			commitIdxList.add(idx);
 		}
		// 持久化，对提交的日志重做，持久化写入
		Iterator<Integer> iterator = commitIdxList.iterator();
		for (LogRecords lr : logRecordsList) {
			if (commitIdx.contains(lr.getTxID()) && lr.getType() == 1) {
				long tag = iterator.next();
				pq.add(tag);
				latestValues.put(lr.getKey(), new TaggedValue(tag, lr.getValue()));
				sm.queueWrite(lr.getKey(), tag, lr.getValue());
			}
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		transactionsets.put(txID, new ArrayList<>());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
		// 创建一个新日志，类型为写操作
		LogRecords currentLogRecords = new LogRecords(1, txID, key, value);
		// 追加日志长度
		currentLogRecords.setSize(16 + 8 + value.length);
		// 日志添加到当前事务集合
		transactionsets.get(txID).add(currentLogRecords);
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		// 创建提交日志 添加到事务集合
		LogRecords commitRecord = new LogRecords(2, txID, -1, null);
		transactionsets.get(txID).add(commitRecord);

		// 存储key和日志大小
		HashMap<Long, Long> tempKey = new HashMap<>();
		for (LogRecords record : transactionsets.get(txID)) {
			// 写操作添加到集合
			if (record.getType() == 1) {
				long logSize = 0;
				// 日志转byte数组，写磁盘
				byte[] bytes = LogRecords.changeToByte(record);
				int size = record.size();
				// 字节数组保存缓冲区
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				for (int i = 0; i < size; i += maxLength) {
					int length = Math.min(size - i, maxLength);
					byte[] temp = new byte[length];
					// 从数组下标i开始，读取length长度缓存到数组temp
					buffer.get(temp, i, length);
					logSize = logManager.appendLogRecord(temp);
				}
				tempKey.put(record.getKey(), logSize);
			} else {
				// 直接追加LogRecord， 提交等操作
				byte[] bytes = LogRecords.changeToByte(record);
				int size = record.size();
				ByteBuffer buffer = ByteBuffer.wrap(bytes);
				for (int i = 0; i < size; i += maxLength) {
					int l = Math.min(size - i, maxLength);
					byte[] temp = new byte[l];
					buffer.get(temp, i, l);
					logManager.appendLogRecord(temp);
				}
			}
		}

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				// 根据日志偏移量添加到latestValues
				long tag = tempKey.get(x.key);
				latestValues.put(x.key, new TaggedValue(tag, x.value));
				// 存储日志位置
				pq.add(tag);
				storageManager.queueWrite(x.key, tag, x.value);
			}
			// 提交后移除
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
		transactionsets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		// redo log
		// 当checkpoint和write pos相遇，表示redo log已满，此时数据库停止更新数据库更新语句的执行，进行redo log日志同步到磁盘
		if (persisted_tag == pq.peek()) {
			// 设置checkpoint是否为当前最早日志
			logManager.setLogTruncationOffset((int)persisted_tag);
		}
		pq.remove(persisted_tag);
	}
}
