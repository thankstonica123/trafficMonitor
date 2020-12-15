package com.thankstonica.flink.service;

import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class GeneratorData {
    public static void main(String[] args) {
        String url = "http://s1:8686/controller/sendData/trafic_data";
        CloseableHttpClient client = HttpClients.createDefault();
        String result = null;
        String[] locations = {"京","津","冀","晋","蒙","辽","吉","黑","沪","苏","浙","皖","闽","赣","鲁","豫"};
        // 模拟一天的数据
        String day = "2020-12-09";
        // 初始化高斯分布对象
        JDKRandomGenerator generator = new JDKRandomGenerator();
        generator.setSeed(new Date().getTime());
        GaussianRandomGenerator gs = new GaussianRandomGenerator(generator);

        Random r = new Random();
        try {
            // 模拟30000台车辆
            for(int m=0;m<30000;m++){
                // 获取车牌号
                String carStr = locations[r.nextInt(locations.length)]+(char)(65+r.nextInt(26))+ String.format("%05d", r.nextInt(10000));
                // 获取小时
                String hour = String.format("%02d", r.nextInt(24));
                // 一天内，一辆车大概率经过30个左右的卡口
                //标准高斯分布，大概率在[-1,1]
                double v = gs.nextNormalizedDouble();
                // 一量车，一天经过卡口的数量
                int count = (int)Math.abs(30+30*v+1);
                for(int j=0;j<count;j++){
                    // if count > 30,为了数据更加真实，使时间+1h
                    if(j %30 == 0){
                        int newHour = Integer.parseInt(hour)+1;
                        if(newHour == 24){
                            newHour = 0;
                        }
                        hour = String.format("%02d", newHour);
                    }
                    // 经过卡口的时间
                    String time = day+" "+hour+":"+String.format("%02d",r.nextInt(60))+":"+String.format("%02d",r.nextInt(60));
                    long actionTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
                    // 卡口ID
                    String monitorId = String.format("%04d", r.nextInt(10000));
                    // 随机道路
                    String roadId = String.format("%02d", r.nextInt(100));
                    String areaId = String.format("%02d", r.nextInt(100));
                    String cameraId = String.format("%05d", r.nextInt(100000));

                    // 随机速度
                    double v2 = gs.nextNormalizedDouble();
                    String speed = String.format("%.1f", Math.abs(40 + (40 * v2)));
                    String data = actionTime+","+monitorId+","+cameraId+","+carStr+","+speed+","+roadId+","+areaId;

                    HttpPost post = new HttpPost(url);
                    post.setHeader("Content-Type","application/json");
                    post.setEntity(new StringEntity(data, Charset.forName("UTF-8")));
                    HttpResponse response = client.execute(post);
                    Thread.sleep(1000);
                    // 响应状态200，获取返回内容
                    if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                        result = EntityUtils.toString(response.getEntity(), "UTF-8");
                        System.out.println(result);
                    }

                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }
}
