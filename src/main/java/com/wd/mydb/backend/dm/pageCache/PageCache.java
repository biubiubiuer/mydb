package com.wd.mydb.backend.dm.pageCache;

import com.wd.mydb.backend.dm.page.Page;
import com.wd.mydb.backend.utils.Panic;
import com.wd.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.wd.mydb.backend.utils.DBConstant.DB_SUFFIX;
import static com.wd.mydb.backend.utils.DBConstant.PAGE_SIZE;

public interface PageCache {
    
    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);
    
    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page page);
    
    static PageCacheImpl create(String path, long memory) {
        File f = new File(path + DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
    
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + DB_SUFFIX);
        if (!f.exists()) {
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
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
    
}
