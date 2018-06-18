package flightdelay1.join.practice;

public class Utils {

    public static boolean replaceNAWithZero(String[] strs){
        if(strs == null || strs.length == 0){
            return false;
        }

        for(int i = 0; i < strs.length; i++){
            if(strs[i].trim().toUpperCase().equals("NA")){
                return true;
            }
        }

        return false;
    }
}
