package ch.furthermore.poorman.ec2cd;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;

public class ClusterDiscoverer {
	public interface ClusterListener {
		void peerDiscovered(String instanceId, String privateIp);
		void peerGone(String instanceId, String privateIp);
	}
	
	private final ClusterListener clusterListener;
	private final AmazonAutoScaling autoScaling;
	private final AmazonEC2Client ec2;
	private final String instanceId;
	private final String autoScalingGroupName;
	private final Map<String,String> peerIpByInstanceId = new HashMap<String, String>();

	public ClusterDiscoverer(ClusterListener clusterListener) throws MalformedURLException, IOException {
		this.clusterListener = clusterListener;
		
		autoScaling = new AmazonAutoScalingClient(new InstanceProfileCredentialsProvider());
		ec2 = new AmazonEC2Client(new InstanceProfileCredentialsProvider());
		
		instanceId = lookupInstanceId();
		
		AutoScalingGroup myAutoScalingGroup = null;
		outer: for (DescribeAutoScalingGroupsResult result = autoScaling.describeAutoScalingGroups(); 
				result.getNextToken() != null;
				result = autoScaling.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest().withNextToken(result.getNextToken())))
		{
			for (AutoScalingGroup autoScalingGroup : result.getAutoScalingGroups()) {
				for (Instance instance : autoScalingGroup.getInstances()) {
					if (instance.getInstanceId().equals(instanceId)) {
						myAutoScalingGroup = autoScalingGroup;
						break outer;
					}
				}
			}			
		}
		
		if (myAutoScalingGroup == null) {
			throw new IllegalStateException("Unable to find my AutoScalingGroup");
		}
		
		autoScalingGroupName = myAutoScalingGroup.getAutoScalingGroupName();
		
		updatePeers(myAutoScalingGroup);
	}
	
	public void updatePeers() {
		DescribeAutoScalingGroupsRequest request = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(autoScalingGroupName);
		updatePeers(autoScaling.describeAutoScalingGroups(request).getAutoScalingGroups().get(0));
	}
	
	private synchronized void updatePeers(AutoScalingGroup autoScalingGroup) {
		List<String> oldPeerInstanceIds = new LinkedList<String>();
		for (Instance instance : autoScalingGroup.getInstances()) {
			if (instanceId.equals(instance.getInstanceId())) continue;
			
			if (oldPeerInstanceIds.contains(instance.getInstanceId())) {
				oldPeerInstanceIds.remove(instance.getInstanceId());
			} else {
				DescribeInstancesResult result = ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId()));
				if (result.getReservations().size() == 1 && result.getReservations().get(0).getInstances().size() == 1) {
					String privateIp = result.getReservations().get(0).getInstances().get(0).getPrivateIpAddress();
					peerIpByInstanceId.put(instance.getInstanceId(),  privateIp);
					
					clusterListener.peerDiscovered(instance.getInstanceId(),  privateIp);
				}
			}
		}
		for (String removedInstanceId : oldPeerInstanceIds) {
			String privateIp = peerIpByInstanceId.get(removedInstanceId);
			peerIpByInstanceId.remove(removedInstanceId);
			
			clusterListener.peerGone(removedInstanceId, privateIp);
		}
	}
	
	@SuppressWarnings("deprecation")
	private static String lookupInstanceId() throws MalformedURLException, IOException { 
		URL metaUrl = new URL("http://169.254.169.254/latest/meta-data/instance-id");
		URLConnection conn = metaUrl.openConnection();
		conn.setConnectTimeout(1500);
		conn.setReadTimeout(1500);
		DataInputStream in = new DataInputStream(conn.getInputStream());
		try {
			return in.readLine();
		}
		finally {
			in.close();
		}
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException, NumberFormatException, InterruptedException {
		ClusterDiscoverer discoverer = new ClusterDiscoverer(new ClusterListener() {
			@Override
			public void peerGone(String instanceId, String privateIp) {
				System.out.println("peerGone(" + instanceId + "," + privateIp + ")");
			}
			
			@Override
			public void peerDiscovered(String instanceId, String privateIp) {
				System.out.println("peerDiscovered(" + instanceId + "," + privateIp + ")");
			}
		});
		String sleepProperty = System.getProperty("sleep");
		if (sleepProperty != null) {
			int sleep = Integer.parseInt(sleepProperty);
			for (;;) {
				Thread.sleep(sleep);
				discoverer.updatePeers();
			}
		}
	}
}
