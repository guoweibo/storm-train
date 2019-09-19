package com.jungle.bigdata.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC Server服务
 */
public class RPCServer {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();

        RPC.Builder builder = new RPC.Builder(configuration);

        // java Builder模式
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();


        server.start();
    }
}
