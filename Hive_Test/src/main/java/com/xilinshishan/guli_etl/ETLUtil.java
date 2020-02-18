package com.xilinshishan.guli_etl;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class ETLUtil {
    /**
     * @param str 输入字符串
     * @return 格式化后的字符串
     * 格式化：
     * 1.按照'\t'分割，数组长度小于9的删除；
     * 2.将类别列中的空格删除；
     * 3.将关联视频ID中的'\t'替换为'&';
     * PF_ZMlw4rHs	DisneyUnleashed	729	Film & Animation	288	96	3.5	2	0
     * 1xbSFrHzFQ0	4VP4qSjDNQs	RJgGeYiJrj0	MqySp7Nq5j0	MC--VwYTHAM	eVdiIbWT60M	Da80HD18tp0	mhfp6Z8z1cI	tdRBH7VBrSY	xKvzxLeY0iQ	_I2EZYCdUXI	gompU_uhYq0	CiPPxBxPBOw	MeFi3SDi_n8	YRi20cWMYOM	v2uEfmWO6z8	2t2Fe_ixWpI	_Wud5vSIQ0I	bBml9opQxnc	1ZU_ytaZTxg
     */
    public static String cleanStr(String str){
        //1.分割数据
        String[] splits = str.split("\t");
        //2.删除长度不足9的数据
        if(splits.length < 9){
            return null;
        }
        //3.将类别列中的空格删除
        splits[3] = splits[3].replaceAll(" ","");
        //4.定义容器
        StringBuffer formatStr = new StringBuffer();
        //5.添加数据
        for (int i = 0; i < splits.length; i++) {
            if(i<=8){
                if(splits.length == 9 && i == 8){
                    formatStr.append(splits[i]);
                }else{
                    formatStr.append(splits[i] + "\t");
                }
            }else{
                if(i == splits.length-1){
                    formatStr.append(splits[i]);
                }else{
                    formatStr.append(splits[i] + "&");
                }
            }
        }
        return formatStr.toString();
    }

//    public static void main(String[] args) {
//        String str = "PF_ZMlw4rHs\tDisneyUnleashed\t729\tFilm & Animation\t288\t96\t3.5\t2\t0\t1xbSFrHzFQ0\t4VP4qSjDNQs\tRJgGeYiJrj0\tMqySp7Nq5j0\tMC--VwYTHAM\teVdiIbWT60M\tDa80HD18tp0\tmhfp6Z8z1cI\ttdRBH7VBrSY";
//        System.out.println(cleanStr(str));
//    }
}
