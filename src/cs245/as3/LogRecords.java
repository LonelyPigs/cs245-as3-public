package cs245.as3;

import java.nio.ByteBuffer;

/**
 * @program: cs245
 * @description: 日志记录
 * @author: lzj
 * @create: 2022-06-10 18:30
 **/
public class LogRecords {

    private int type; // 0开始 1写 2提交 3回滚

    private long txID;

    private long key;

    private byte[] value; // 值

    private int size; // 日志大小 type + txID + size + value = 4 + 8 + 4 + 8 + x

    public LogRecords(int type, long txID, long key, byte[] value) {
        this.type = type;
        this.txID = txID;
        this.key = key;
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getTxID() {
        return txID;
    }

    public void setTxID(long txID) {
        this.txID = txID;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int size() {
        size = 16;
        if (type == 1) {
            size = size + 8 + value.length;
        }
        return size;
    }

    // 日志转byte
    public static byte[] changeToByte(LogRecords logRecords) {
        // 分配内存
        ByteBuffer record = ByteBuffer.allocate(128);
        record.putLong(logRecords.txID);
        record.putInt(logRecords.type);
        record.putInt(logRecords.size());
        if (logRecords.type == 1) {
            record.putLong(logRecords.key);
            record.put(logRecords.value);
        }
        byte[] result = record.array();
        return result;
    }

    // byte转日志
    public static LogRecords changeToLogRecord(byte[] recordBytes) {
        ByteBuffer buff = ByteBuffer.wrap(recordBytes);
        long id = buff.getLong();
        LogRecords record = new LogRecords(buff.getInt(), id, -1, null);
        record.size = buff.getInt();
        if (record.type == 1) {
            record.key = buff.getLong();
            int size = record.size - 24;
            record.value = new byte[size];
            for (int i = 0; i < size; ++i) {
                record.value[i] = buff.get();
            }
        }
        return record;
    }

}
