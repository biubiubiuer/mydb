package com.wd.mydb.backend.dm.page;

import com.wd.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

import static com.wd.mydb.backend.dm.pageCache.PageCache.PAGE_SIZE;


/**
 * 特殊管理第一页
 * ValidCheck
 * db 启动时会给 100~107 字节处填入一个随机字节, db 关闭时将其拷贝到 100~115 字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;
    
    public static byte[] InitRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }
    
    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }
    
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(
                RandomUtil.randomBytes(LEN_VC), 0,
                raw, OF_VC, 
                LEN_VC
        );
    }
    
    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }
    
    private static void setVcClose(byte[] raw) {
        System.arraycopy(
                raw, OF_VC,
                raw, OF_VC + LEN_VC,
                LEN_VC
        );
    }
    
    public static boolean checkvC(Page page) {
        return checkvC(page.getData());
    }
    
    private static boolean checkvC(byte[] raw) {
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC), 
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + (LEN_VC << 1))
        );
    }
}
