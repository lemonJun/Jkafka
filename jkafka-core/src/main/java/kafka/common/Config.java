package kafka.common;

import kafka.utils.Logging;

/**
 * Created by Administrator on 2017/4/4.
 */
public class Config extends Logging {

    public void validateChars(String prop, String value) {
        String legalChars = "[a-zA-Z0-9\\._\\-]";
        if (!value.matches(legalChars + "*")) {
            throw new InvalidConfigException(prop + " " + value + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }
}