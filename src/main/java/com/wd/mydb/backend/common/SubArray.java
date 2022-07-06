package com.wd.mydb.backend.common;

import lombok.AllArgsConstructor;

/**
 * 共享内存数组
 */
@AllArgsConstructor
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;
}
