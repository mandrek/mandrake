package fun.messaging.distributed;

import java.io.Serializable;

/**
 * Could be easily converted to a protobuf
 * Created by kuldeep
 */
public class HandShake implements Serializable {
    private static final long serialVersionUID = 1;


    public static enum HANDHAKE_TYPE {QUERY, SUBSCRIBE, SUBSCRIBE_AND_QUERY, REGISTER}

    private final HANDHAKE_TYPE handShakeType;
    private final String handShakeMsg;
    private final byte[] expression;

    public HandShake(HANDHAKE_TYPE handShakeType, String handShakeVal) {
        this(handShakeType, handShakeVal, new byte[0]);
    }

    public HandShake(HANDHAKE_TYPE handShakeType, String handShakeVal, byte[] msg) {
        this.handShakeType = handShakeType;
        this.handShakeMsg = handShakeVal;
        this.expression = msg;
    }

    public HANDHAKE_TYPE getHandShakeType() {
        return handShakeType;
    }

    public String getHandShakeMsg() {
        return handShakeMsg;
    }

    public byte[] getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        return handShakeType.name() + "->" + handShakeMsg;
    }
}
