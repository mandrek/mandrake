package fun.messaging.distributed;

import java.io.Serializable;

/**
 * Could be easily converted to a protobuf msg
 * Created by kuldeep
 */
public class HandShake implements Serializable {
    private static final long serialVersionUID = 1;


    public static enum HANDHAKE_TYPE {QUERY, SUBSCRIBE, SUBSCRIBE_AND_QUERY, REGISTER}

    private final HANDHAKE_TYPE handShakeType;
    private final String handShakeMsg;

    public HandShake(HANDHAKE_TYPE handShakeType, String handShakeVal) {
        this.handShakeType = handShakeType;
        this.handShakeMsg = handShakeVal;
    }


}
