package com.wd.mydb.backend.dm.page;

import com.wd.mydb.backend.utils.Parser;

import java.util.Arrays;

import static com.wd.mydb.backend.dm.pageCache.PageCache.PAGE_SIZE;

/**
 * PageX 管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PAGE_SIZE - OF_DATA;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }
    
    /**
     * 将 raw 插入 pg 中, 返回插入位置
     * @param page
     * @param raw
     * @return
     */
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }
    
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0,
                raw, OF_FREE, 
                OF_DATA
        );
    }

    /**
     * 获取 page 的 FSO
     * @param raw
     * @return
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 获取页面的空闲空间大小
     * @param page
     * @return
     */
    public static int getFreeSpace(Page page) {
        return PAGE_SIZE - (int) getFSO(page.getData());
    }

    /**
     * 将 raw 插入 pg 中的 offset 位置, 并将 pg 的 offset 设置为较大的 offset
     * @param page
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        
        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 将 raw 插入 pg 中的 offset 位置, 不更新 update
     * @param page
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }
    
}
