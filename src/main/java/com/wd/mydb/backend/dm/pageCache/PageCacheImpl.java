package com.wd.mydb.backend.dm.pageCache;

import com.wd.mydb.backend.common.AbstractCache;
import com.wd.mydb.backend.dm.page.Page;
import com.wd.mydb.backend.dm.page.PageImpl;
import com.wd.mydb.backend.utils.Panic;
import com.wd.mydb.common.Error;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEN_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";
    
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock fileLock;

    /**
     * PageCache 还使用礼一个 AtomicInteger, 
     * 来记录当前打开的数据库文件有多少页. 
     * 这个数字在数据库文件呗打开时就会计算, 并在新建页面时自增. 
     */
    private AtomicInteger pageNumbers;
    
    public PageCacheImpl(RandomAccessFile raf, FileChannel fc, int maxResource) {
        super(maxResource);
        if (maxResource < MEN_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = raf; 
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    /**
     * 由于数据源就是文件系统, 
     * getForCache() 直接从文件中读取, 并包裹成 Page 即可
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset =PageCacheImpl.pageOffset(pgno);

        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf.nioBuffer());
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.nioBuffer().array(), this);
    }
    
    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }

    /**
     * releaseForCache() 驱逐页面时, 
     * 需要根据页面是否是脏页面, 来决定是否写会文件系统
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }
    
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);
        
        fileLock.lock();
        try {
            ByteBuf buf = Unpooled.wrappedBuffer(pg.getData());
            fc.position(offset);
            fc.write(buf.nioBuffer());
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long) pgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }
}
