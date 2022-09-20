package com.xzz.chapter07;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

/**
 * @author 徐正洲
 * @date 2022/9/19-20:10
 */
public class CustomSource extends RichSourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] name = {"jack", "mark", "wang", "xu","li","tai","hadoop"};
        String[] url = {"www.baidu.com","www.taobao.com","www.jingdong.com","www.oceandatum.com","www.huawei.com"};
        Random random = new Random();
        while (true){
            sourceContext.collect(new Event(name[random.nextInt(name.length)],url[random.nextInt(url.length)],System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}