package com.jungle.bigdata.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * RPC客户端
 */
public class RPCClient {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        long clientVersion = 88888888;

        UserService userService = RPC.getProxy(UserService.class, clientVersion,
                new InetSocketAddress("localhost", 9999),
                configuration);

        userService.addUser("zhangsan",30);
        System.out.println("From client... invoked");

        RPC.stopProxy(userService);

    }
}
