package com.jungle.storm.web.controller;

import com.jungle.storm.web.domain.ResultBean;
import com.jungle.storm.web.service.ResultBeanService;
import net.sf.json.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Controller
public class StatApp {


    @Autowired
    ResultBeanService resultBeanService;


//    @RequestMapping(value = "/index", method = RequestMethod.GET)
    public ModelAndView map_stat() {

        ModelAndView view = new ModelAndView("index");
        List<ResultBean> results = resultBeanService.query();

        JSONArray jsonArray = JSONArray.fromObject(results);

        System.out.println(jsonArray);


        // 如何把我们从后台查询到的数据以json的方式返回给前台页面
        view.addObject("data_json", jsonArray);
        return view;
    }


    @RequestMapping("/test")
    public String test(Model model){


        List<ResultBean> results = resultBeanService.query();

        JSONArray jsonArray = JSONArray.fromObject(results);

        System.out.println(jsonArray);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        System.out.println(df.format(new Date()));// new Date()为获取当前系统时间

        model.addAttribute("data_json",jsonArray);
//        model.addAttribute("books","ate");
//        System.out.println("test");
        return "index";
    }


}
