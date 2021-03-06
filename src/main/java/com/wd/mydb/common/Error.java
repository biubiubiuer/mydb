package com.wd.mydb.common;

public class Error {

    /**
     * common
     */
    public static final Exception CacheFullException = new RuntimeException("Cache is fill!");
    public static final Exception FileExistsException = new RuntimeException("File Already exists!");
    public static final Exception FileNotExistsException = new RuntimeException("File does not exists!");
    public static final Exception FileCannotRWException = new RuntimeException("File can not read or write!");

    /**
     * tm
     */
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID File!");

    /**
     * dm
     */
    public static final Exception MemTooSmallException = new RuntimeException("Memory too small!");
    public static final Exception BadLogFileException = new RuntimeException("Bad Log File!");
    public static final Exception DataTooLargeException = new RuntimeException("Data too large!");
    public static final Exception DatabaseBusyException = new RuntimeException("Database is busy!");
    
}
