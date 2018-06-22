package flight2.maximum.delay;

public class Utils {

    public static boolean replaceNAwithZero(String[] strs){
        if(strs == null || strs.length == 0){
            return false;
        }

        for (String str : strs ) {
            if(str.trim().equalsIgnoreCase("NA")){
                return true;
            }
        }

        return false;
    }

}
