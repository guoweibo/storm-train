package com.jungle.storm.web.config;

import com.jungle.storm.web.controller.DeleteJob;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Quartz Configuration.
 *
 * @since 1.0.0 2017年11月23日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
@Configuration
public class QuartzConfiguration {


    // JobDetail
    @Bean
    public JobDetail weatherDataSyncJobDetail() {
        return JobBuilder.newJob(DeleteJob.class).withIdentity("DeleteJob")
                .storeDurably().build();
    }

    // Trigger
    @Bean
    public Trigger deleteDataSyncTrigger() {
        //2秒执行一次
        SimpleScheduleBuilder schedBuilder = SimpleScheduleBuilder.simpleSchedule()
                .withIntervalInSeconds(120).repeatForever();

        return TriggerBuilder.newTrigger().forJob(weatherDataSyncJobDetail())
                .withIdentity("deleteDataSyncTrigger").withSchedule(schedBuilder).build();
    }
}
