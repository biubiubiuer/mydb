package com.wd.mydb.backend.tm;

import com.wd.mydb.backend.common.Error;
import com.wd.mydb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.wd.mydb.backend.utils.DBConstant.LEN_XID_HEADER_LENGTH;
import static com.wd.mydb.backend.utils.DBConstant.XID_SUFFIX;

public interface TransactionManager {
    long begin();  // 开启一个新事物
    void commit(long xid);  // 提交一个事物
    void abort(long xid);  // 取消一个事物
    boolean isActive(long xid);  // 查询一个事物的状态是否是正在进行的状态
    boolean isCommitted(long xid);  // 查询一个事物的状态是否是已提交
    boolean isAborted(long xid);  // 查询一个事物d的状态是否是已取消
    void close();  // 关闭 TM
    
    public static TransactionManagerImpl create(String path) {
        File f = new File(path + XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        
        // 写空 XID 文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[LEN_XID_HEADER_LENGTH]);
    }
}
