package com.wd.mydb.backend.dm.logger;

import com.wd.mydb.backend.utils.Panic;
import com.wd.mydb.backend.utils.Parser;
import com.wd.mydb.common.Error;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.wd.mydb.backend.dm.logger.LoggerImpl.LOG_SUFFIX;

public interface Logger {
    
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();
    
    static Logger create(String path) {
        File f = new File(path + LOG_SUFFIX);
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

        ByteBuf buf = Unpooled.wrappedBuffer(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf.nioBuffer());
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        return new LoggerImpl(raf, fc, 0);
    }
    
    static Logger open(String path) {
        File f = new File(path + LOG_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
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
        
        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        
        return lg;
    }
    
}
