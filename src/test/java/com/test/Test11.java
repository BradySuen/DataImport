package com.test;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;

/**
 * @author BradySuen
 * @create_time 2021/9/16
 * @description
 **/
public class Test11 {
    public static void main(String[] args) {
        String aa = "{\"ad_platform\":\"11\",\"adid\":\"11\",\"agency_id\":\"11\",\"android_id\":\"11\",\"app_version\":\"1.0.0\",\"attribution_module\":\"11\",\"campaign_id\":\"11\",\"campaign_name\":\"11\",\"channel_type\":\"11\",\"city\":\"11\",\"cost\":\"0.5\",\"cost_type\":\"11\",\"country_code\":\"GB\",\"creative_id\":\"111\",\"device_limit_tracking\":\"1\",\"device_marketing_name\":\"iPhone\",\"device_model\":\"iPhone\",\"device_os_name\":\"iOS\",\"device_os_version\":\"14.6.0\",\"device_ua\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148\",\"device_ver\":\"iPhone-iOS-14.6.0\",\"dma_code\":\"\",\"duplicate\":\"\",\"identifiers\":\"2222222\",\"identity_link\":\"\",\"idfa\":\"\",\"idfv\":\"D5857FC4-EF8D-482F-ACBB-B0BE6F59EA8E\",\"install_att\":\"false\",\"install_att_detail\":\"denied\",\"install_att_duration_sec\":\"0\",\"install_att_time\":\"\",\"ip_address\":\"85.255.232.185\",\"kochava_device_id\":\"KI1626462209T687D055782D84E3F8AC349756EC37639\",\"latitude\":\"51.4964\",\"longitude\":\"-0.1224\",\"match_object\":\"\",\"matched_to_id\":\"\",\"matched_to_type\":\"\",\"network_id\":\"\",\"network_name\":\"\",\"postal_code\":\"\",\"properties\":\"3123131231231231\",\"region\":\"\",\"sdk_version\":\"iOSTracker 4.6.1\",\"segment_name\":\"\",\"site_id\":\"\",\"tracker_guid\":\"\",\"tracker_id\":\"\",\"tracker_name\":\"\",\"traffic_verification_fail_reason\":\"\",\"traffic_verified\":\"unverified\",\"valid_receipt\":\"verified\",\"waterfall_level\":\"\"}";
        JSONObject jsonObject = JSONObject.parseObject(aa);
        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("aaaa",jsonObject);
        jsonObject2.getJSONObject("aaaa").fluentRemove("ad_platform");
        System.out.println(jsonObject2);
        System.out.println("=====================");
        System.out.println(jsonObject);


        ArrayList<String> aaa = new ArrayList<String>();
        System.out.println(ArrayList.class.asSubclass(String.class));


    }
}
