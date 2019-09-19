package com.jungle.bigdata.drpc;

/**
 * 用户的服务
 */
public interface UserService {

    //序列化
    public static final long versionID = 8888888;
    /**
     * 添加用户
     * @param name 名字
     * @param age 年龄
     */
    public void addUser(String name,int age);
}
