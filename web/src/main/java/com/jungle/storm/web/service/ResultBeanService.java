package com.jungle.storm.web.service;

import com.jungle.storm.web.domain.ResultBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class ResultBeanService {


    //使用jdbc框架
    @Autowired
    JdbcTemplate jdbcTemplate;


    public List<ResultBean> query() {

        String sql = "select longitude,latitude ,count(1) as c from stat where time > unix_timestamp(date_sub(current_timestamp(), interval 5 minute))*1000 group by longitude,latitude";

        return (List<ResultBean>) jdbcTemplate.query(sql, new RowMapper<ResultBean>() {

            @Override
            public ResultBean mapRow(ResultSet resultSet, int i) throws SQLException {
                ResultBean bean = new ResultBean();

                bean.setLng(resultSet.getDouble("longitude"));
                bean.setLat(resultSet.getDouble("latitude"));
                bean.setCount(resultSet.getLong("c"));

                return bean;
            }
        });
    }

    public void delete() {
        String sql = "delete from stat where time < unix_timestamp(date_sub(current_timestamp(),interval 20 minute))*1000";
        jdbcTemplate.execute(sql);
    }
}