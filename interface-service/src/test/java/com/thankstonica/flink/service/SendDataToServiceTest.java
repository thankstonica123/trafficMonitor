package com.thankstonica.flink.service;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class SendDataToServiceTest {
    public static void main(String[] args) {

        String url = "http://localhost:8686/controller/sendData/trafic_data";
        CloseableHttpClient client = HttpClients.createDefault();
        String result = null;
        try {
            for(int i=0;i<20;i++){
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type","application/json");
                String data = "1,2,3,京P12345,52.1,"+i;
                post.setEntity(new StringEntity(data,Charset.forName("UTF-8")));
                HttpResponse response = client.execute(post);
                Thread.sleep(1000);
                // 响应状态200，获取返回内容
                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
                    result = EntityUtils.toString(response.getEntity(), "UTF-8");
                    System.out.println(result);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}























