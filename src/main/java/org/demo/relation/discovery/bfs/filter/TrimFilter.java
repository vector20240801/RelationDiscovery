package org.demo.relation.discovery.bfs.filter;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

/**
 * @author vector
 * @date 2023-08-12 14:50
 */
@Service("TrimFilter")
public class TrimFilter implements ValueFilter {
    private static final long serialVersionUID = 1518351533818955340L;

    @Override
    public Object filter(String columnName, Object originValue) {
        if (!(originValue instanceof String)) {
            return originValue;
        }
        if (StringUtils.isEmpty((CharSequence) originValue)) {
            return originValue;
        }
        String ret = StringUtils.replaceAll((String) originValue, "^0+", "");
        if (StringUtils.isEmpty(ret)) {
            return "0";
        }
        return ret;
    }
}
