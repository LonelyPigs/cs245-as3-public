package cs245.as3;

/**
 * @program: cs245
 * @description: 日志工具
 * @author: lzj
 * @create: 2022-06-10 18:40
 **/
public class LogUtils {
    public final static int sizeOffset = 12;
    public int getLogSize(byte[] bytes) {
        return (((bytes[sizeOffset] & 0xff) << 24) |
                ((bytes[sizeOffset + 1] & 0xff) << 16) |
                ((bytes[sizeOffset + 2] & 0xff) << 8) |
                (bytes[sizeOffset + 3] & 0xff));

    }
}
