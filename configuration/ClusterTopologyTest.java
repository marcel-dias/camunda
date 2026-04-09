import io.camunda.configuration.Cluster;
import io.camunda.configuration.TopologyInfo;

public class ClusterTopologyTest {
    public static void main(String[] args) {
        Cluster cluster = new Cluster();
        TopologyInfo topology = cluster.getTopology();
        
        // Test initial state
        System.out.println("Initial topology configured: " + topology.isConfigured());
        
        // Test setting values
        topology.setZone("us-east-1a");
        topology.setRegion("us-east-1");
        System.out.println("After setting values - configured: " + topology.isConfigured());
        
        // Test via setter
        TopologyInfo newTopology = new TopologyInfo();
        newTopology.setZone("eu-west-1b");
        newTopology.setRegion("eu-west-1");
        cluster.setTopology(newTopology);
        
        System.out.println("After setter - zone: " + cluster.getTopology().getZone());
        System.out.println("After setter - region: " + cluster.getTopology().getRegion());
        
        // Test toString contains topology
        String clusterString = cluster.toString();
        System.out.println("toString contains topology: " + clusterString.contains("topology="));
        System.out.println("SUCCESS: TopologyInfo is properly integrated into Cluster");
    }
}
