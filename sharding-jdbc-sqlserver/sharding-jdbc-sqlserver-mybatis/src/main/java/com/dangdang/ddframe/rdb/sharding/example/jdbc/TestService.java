package com.dangdang.ddframe.rdb.sharding.example.jdbc;

import com.dangdang.ddframe.rdb.sharding.example.jdbc.entity.Order;
import com.dangdang.ddframe.rdb.sharding.example.jdbc.service.OrderService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:META-INF/mybatisContext.xml")
public class TestService {
    @Autowired
    private OrderService orderService;

    @Test
    public void testClear() {
        orderService.clear();
    }

    @Test
    public void testService() {
        orderService.clear();
        orderService.fooService();
        List<Order> orders = orderService.select();
        Assert.assertEquals(orders.size(), 2);
    }

    @Test
    public void testServiceFailure() {
        orderService.clear();
        try {
            orderService.fooServiceWithFailure();
        } catch (Exception e) {
            System.out.println("roll back");
        }
        List<Order> orders = orderService.select();
        Assert.assertEquals(orders.size(), 0);
    }

    @Test
    public void testSelectTop() {
        orderService.printTop(5);
    }


    @Test
    public void testSelectRownumber() {
        orderService.printRownumber(2,3);
    }
}
