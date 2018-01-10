package com.rbc.rbcone.data.rest.kafka.util;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class RandomizeTimeStamp {

    public static Date getRandom() {
        Calendar now = Calendar.getInstance();
        now.add(Calendar.SECOND, new Random().nextInt(3600) * -1);
        return now.getTime();
    }
}
