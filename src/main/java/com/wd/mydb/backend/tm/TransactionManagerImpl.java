package com.wd.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

import com.wd.mydb.common.Error;
import com.wd.mydb.backend.utils.Panic;
import com.wd.mydb.backend.utils.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import static com.wd.mydb.backend.utils.DBConstant.*;

public class TransactionManagerImpl implements TransactionManager{

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        this.xidCounter = xidCounter;
        this.counterLock = counterLock;
    }

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;


    /**
     * 检查 XID 文件是否合法
     * 读取 XID_FIELD_HEADER 中的 xidCounter, 根据它计算文件的理论长度, 对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }

    }
    
    /**
     * 根据事物 xid 取得其在 xid 文件中对应的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }


    /**
     * 开启一个新事物, 并返回 XID
     * @return
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 更新 xid 事物的状态为 status
     * @param xid
     * @param status
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将 XID 加一, 并更新 XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuf buf = Unpooled.wrappedBuffer(Parser.long2Byte(xidCounter)); 
        try {
            fc.position(0);
            fc.write(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 提交 XID 事物
     * @param xid
     */
    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚 XID 事物
     * @param xid
     */
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检查 XID 事物是否处于 status 状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
    
    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) {
            return true;
        }
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
