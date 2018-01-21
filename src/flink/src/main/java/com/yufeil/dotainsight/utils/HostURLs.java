
/*
    Saves the HostURLs of Kafka, Flink, Zookeeper, Cassandra, Redis
 */

public class HostURLs {
    private String ZOOKEEPER_URL = System.getenv("ZOOKEEPER_URL");
    private String KAFKA_URL = System.getenv("KAFKA_URL");
    private String FLINK_URL = System.getenv("FLINK_URL");
    private String CASSANDRA_URL = System.getenv("CASSANDRA_URL");
    private String REIDS_URL = System.getenv("REIDS_URL");

    public String getZOOKEEPER_URL() {
        return ZOOKEEPER_URL;
    }

    public void setZOOKEEPER_URL(String ZOOKEEPER_URL) {
        this.ZOOKEEPER_URL = ZOOKEEPER_URL;
    }

    public String getKAFKA_URL() {
        return KAFKA_URL;
    }

    public void setKAFKA_URL(String KAFKA_URL) {
        this.KAFKA_URL = KAFKA_URL;
    }

    public String getFLINK_URL() {
        return FLINK_URL;
    }

    public void setFLINK_URL(String FLINK_URL) {
        this.FLINK_URL = FLINK_URL;
    }

    public String getCASSANDRA_URL() {
        return CASSANDRA_URL;
    }

    public void setCASSANDRA_URL(String CASSANDRA_URL) {
        this.CASSANDRA_URL = CASSANDRA_URL;
    }

    public String getREIDS_URL() {
        return REIDS_URL;
    }

    public void setREIDS_URL(String REIDS_URL) {
        this.REIDS_URL = REIDS_URL;
    }
}
