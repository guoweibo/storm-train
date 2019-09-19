package com.jungle.storm.web.controller;

import com.jungle.storm.web.service.ResultBeanService;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class DeleteJob extends QuartzJobBean {

    private final static Logger logger = LoggerFactory.getLogger(DeleteJob.class);

    @Autowired
    ResultBeanService resultBeanService;
    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        resultBeanService.delete();
        logger.info("删除前20分钟的数据");

    }
}
