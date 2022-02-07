package net.xdclass.project;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLog {
    /**
     * 用户行为日志user_log.csv，日志中的字段定义如下：
     * 1. user_id | 买家id
     * 2. item_id | 商品id
     * 3. cat_id | 商品类别id
     * 4. merchant_id | 卖家id
     * 5. brand_id | 品牌id
     * 6. month | 交易时间:月
     * 7. day | 交易事件:日
     * 8. action | 行为,取值范围{0,1,2,3},0表示点击，1表示加入购物车，2表示购买，3表示关注商品
     * 9. age_range | 买家年龄分段：1表示年龄<18,2表示年龄在[18,24]，3表示年龄在[25,29]，4表示年龄在[30,34]，5表示年龄在[35,39]，6表示年龄在[40,49]，7和8表示年龄>=50,0和NULL则表示未知
     * 10. gender | 性别:0表示女性，1表示男性，2和NULL表示未知
     * 11. province| 收获地址省份
     *
     * */
    private String user_id;
    private String item_id;
    private String cat_id;
    private String merchant_id;
    private String brand_id;
    private String month;
    private String day;
    private String action;
    private String age_range;
    private String gender;
    private String province;

    @Override
    public String toString() {
        return "UserLog{" +
                "user_id='" + user_id + '\'' +
                ", item_id='" + item_id + '\'' +
                ", cat_id='" + cat_id + '\'' +
                ", merchant_id='" + merchant_id + '\'' +
                ", brand_id='" + brand_id + '\'' +
                ", month='" + month + '\'' +
                ", day='" + day + '\'' +
                ", action='" + action + '\'' +
                ", age_range='" + age_range + '\'' +
                ", gender='" + gender + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}
