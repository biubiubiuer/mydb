package com.wd.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.wd.mydb.backend.utils.Panic;
import com.wd.mydb.backend.utils.Parser;
import com.wd.mydb.common.Error;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为:
 * [XCheckSum] [Log1] [Log2] ... [LogN] [BadTail]
 * XCheckSum 为后续所有日志计算的 CheckSum, int 类型
 * 
 * 每条正确日志的格式为:
 * [Size] [CheckSum] [Data]
 * Size 4 字节 int, 标识 Data 长度
 * CheckSum 4 字节 int
 */
public class LoggerImpl implements Logger {
    
    private static final int SEED = 13331;
    
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";
    
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    /**
     * 当前日志指针的位置
     */
    private long position;

    /**
     * 初始化时记录, log操作不更新
     */
    private long fileSize;
    
    private int xCheckSum;

    public LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.raf = raf;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }
    
    void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
        
        ByteBuf raw = ByteBufAllocator.DEFAULT.buffer(4);
        try {
            fc.position(0);
            fc.read(raw.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xCheckSum = xCheckSum;
        
        checkAndRemoveTail();
    }

    /**
     * 向日志文件写入日志时, 也是首先将数据包裹成日志格式, 
     * 写入文件后, 再更新文件的校验和, 
     * 更新校验和时, 会刷新缓冲区, 保证内容写入磁盘
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuf buf = Unpooled.wrappedBuffer(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    @Override
    public void truncate(long position) throws IOException {
        lock.lock();
        try {
            fc.truncate(position);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 在打开一个日志文件时, 需要首先校验日志文件的 xCheckSum, 并移除文件尾部可能存在的 badTail, 
     * 由于 badTail 该条日志尚未写入完成, 文件的校验和也就不会包含该日志的校验和, 
     * 去掉 badTail 即可保证日志文件的一致性
     */
    private void checkAndRemoveTail() {
        rewind();
        
        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) {
                break;
            }
            xCheck = calCheckSum(xCheck, log);
        }
        if (xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            // 截断文件到正常日志的末尾
            truncate(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            raf.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }
    
    /**
     * 单条日志的校验和, 通过一个指定的种子实现
     * 这样, 对所有日志求初校验和, 求和就能得到日志文件的校验和了
     * @param xCheck
     * @param log
     * @return
     */
    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * Logger 被实现成迭代器模式, 通过 next() 方法, 
     * 不断从文件中读取下一条日志, 并将其中的 Data 解析出来并返回. 
     * next() 方法的实现主要依靠 internNext(), 大致如下, 
     * 其中 position 是当前日志文件读到的位置偏移.
     * @return
     */
    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        
        // 读取 size
        ByteBuf tmp = ByteBufAllocator.DEFAULT.buffer(4);
        try {
            fc.position(position);
            fc.read(tmp.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + size + OF_DATA >= fileSize) {
            return null;
        }
        
        // 读取 checkSum + data
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        byte[] log = buf.array();
        
        // 校验 checkSum
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    private void updateXCheckSum(byte[] log) {
        this.xCheckSum = calCheckSum(this.xCheckSum, log);
        try {
            fc.position(0);
            fc.write(Unpooled.wrappedBuffer(Parser.int2Byte(xCheckSum)).nioBuffer());
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calCheckSum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checkSum, data);
    }

}
