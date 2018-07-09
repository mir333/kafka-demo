package im.ligas.kafka;

import org.apache.kafka.common.utils.AppInfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jeremy on 5/3/16.
 */
class VersionUtil {

    private static Logger LOG = LoggerFactory.getLogger(VersionUtil.class);

    public static String getVersion() {
        try {
            String version = AppInfoParser.getVersion();
            LOG.debug(version);
            return version;
        } catch (Exception ex) {
            return "0.0.0.0";
        }
    }
}
