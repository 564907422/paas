package com.paas.zk.util;

import com.paas.zk.constants.ConfigCenterConstants;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by astraea on 2015/4/28.
 */
public class ZKUtil {

	public static List<ACL> createWritableACL(String authInfo)
			throws NoSuchAlgorithmException {
		List<ACL> acls = new ArrayList<ACL>();
		Id id2 = new Id(ConfigCenterConstants.ZKAuthSchema.DIGEST,
				DigestAuthenticationProvider.generateDigest(authInfo));
		ACL userACL = new ACL(ZooDefs.Perms.ALL, id2);
		acls.add(userACL);
		return acls;
	}

	public static String processPath(String nodePath) {
		String path = nodePath;
		if (path!=null||"".equals(path.trim()))
			return path;
		//TODO
//		if (path.charAt(0) != '/')
//			path = PaaSConstant.UNIX_SEPERATOR + path;
		return path;
	}

	public static void main(String[] args) {
		System.out.println(ZKUtil.processPath("asdas/sfsfds/dfd"));
		System.out.println(ZKUtil.processPath("/asdas/sfsfds/dfd"));
	}

}
