package com.jungle.bigdata.drpc;

public class UserServiceImpl implements UserService {
    @Override
    public void addUser(String name, int age) {
        System.out.println("From Server Invoker:add user success...,name is:"+ name);
    }
}
