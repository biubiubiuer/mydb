package com.wd.mydb.backend.utils;

public class DBConstant {
    
    /**
     * XID 文件头长度
     */
    public static final int LEN_XID_HEADER_LENGTH = 8;

    /**
     * 每个事物的占用长度
     */
    public static final int XID_FIELD_SIZE = 1;

    /**
     * 事物的三种状态
     */
    public static final byte FIELD_TRAN_ACTIVE = 0;
    public static final byte FIELD_TRAN_COMMITTED = 1;
    public static final byte FIELD_TRAN_ABORTED = 2;

    /**
     * 超级事物, 永远为 committed 状态
     */
    public static final long SUPER_XID = 0;
    
    public static final String XID_SUFFIX = ".xid";

    /**
     * Seed, 默认值为 13331
     */
    public static final long SEED = 13331;

    /**
     * 默认数据页大小为 8K
     */
    public static final int PAGE_SIZE = 1 << 13;
    
    public static final String DB_SUFFIX = ".db";
    
}
