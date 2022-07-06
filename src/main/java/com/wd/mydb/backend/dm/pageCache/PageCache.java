package com.wd.mydb.backend.dm.pageCache;

import com.wd.mydb.backend.dm.page.Page;

import java.io.IOException;

public interface PageCache {
    
    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);
    
    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page page);
    
}
