#!/usr/bin/env groovy
import com.hazelcast.stabilizer.coach.Coach

class Cluster {

    def COMMAND = "startmachines"
    def CLUSTER_SIZE = "small"
    def INSTANCE_COUNT = 1
    def COACH_PORT = '8701'
    def config
    def STABILIZER_HOME
    def machineListFile = new File("machine_list.txt")

    List<String> privateIps = []

    Cluster() {
        def props = new Properties()

        new File("start.properties").withInputStream {
            stream -> props.load(stream)
        }
        config = new ConfigSlurper().parse(props)

        machineListFile.text.eachLine { String line -> privateIps << line }

        STABILIZER_HOME = new File(Cluster.class.protectionDomain.codeSource.location.path).parentFile.parent
        echo "STABILIZER_HOME $STABILIZER_HOME"
    }

    void installCoach(String ip) {
        echo "=============================================================="
        echo "Installing Coach on ${ip}"
        echo "=============================================================="

        String members = ""
        privateIps.each { String memberIp -> members += "<member>$memberIp:$COACH_PORT</member>\n" }

        "cp coach-hazelcast-template.xml coach-hazelcast.xml".execute()
        "cat coach-hazelcast.xml | sed -e \"s:MEMBERS:$members:\" > coach-hazelcast.xml.bak && mv coach-hazelcast.xml.bak coach-hazelcast.xml".execute()

        echo "Installing missing Java"
        //install java under Ubuntu.
        ssh(ip, "sudo apt-get update || true")
        ssh(ip, "sudo apt-get install -y openjdk-7-jdk || true")

        echo "Copying stabilizer files"
        println "scp -i ${config.LICENSE} -q -r ${STABILIZER_HOME} ${config.USER}@$ip:"
        "scp -i ${config.LICENSE} -r ${STABILIZER_HOME} ${config.USER}@$ip:".execute()
        //we need to override the hazelcast config file with the one we generated.
        "scp -i ${config.LICENSE} coach-hazelcast.xml ${config.USER}@$ip:hazelcast-stabilizer-0.1-SNAPSHOT/conf/".execute()

        echo "=============================================================="
        echo "Successfully installed Coach on ${ip}"
        echo "=============================================================="
    }

    void installCoaches() {
        privateIps.each { String ip -> installCoach(ip) }
    }

    void startCoaches(){
        echo "=============================================================="
        echo "Starting Coaches"
        echo "=============================================================="

        privateIps.each { String ip ->
            ssh(ip,  "killall -9 java || true")
        }

        privateIps.each { String ip ->
            ssh(ip,  "killall -9 java ; nohup hazelcast-stabilizer-0.1-SNAPSHOT/bin/coach  > coach.out 2> coach.err < /dev/null &")
        }

        echo "=============================================================="
        echo "Coaches started"
        echo "=============================================================="
    }

    void ssh(String ip, String command) {
        String sshCommand = " ssh -i ${config.LICENSE} -q -o StrictHostKeyChecking=no ${config.USER}@$ip \"$command\""
        echo sshCommand
        Process p = sshCommand.execute()
        if (p.waitFor() != 0) {
            //echo "Failed to ssh to $ip"
            //println p.text
        }
    }

    void spawnMachines() {
        echo "echo Starting ${INSTANCE_COUNT} ${config.INSTANCE_TYPE} machines"

        def output = """ec2-run-instances \
        --availability-zone ${config.AVAILABILITY_ZONE} \
        --instance-type ${config.INSTANCE_TYPE} \
        --instance-count $INSTANCE_COUNT \
        --group ${config.SECURITY_GROUP} \
        --key ${config.KEY_PAIR} \
        ${config.AMI}""".execute().text

        echo "=============================================================="
        echo output
        echo "=============================================================="

        def ids = []
        output.eachLine { String line, count ->
            if (line.startsWith("INSTANCE")) {
                def id = line.split()[1]
                ids << id
            }
        }

        awaitStartup(ids)

        echo "=============================================================="
        echo "Successfully started ${INSTANCE_COUNT} ${config.INSTANCE_TYPE} machines "
        echo "=============================================================="

        initPrivateIps(ids)

        echo "Coaches started"
        privateIps.each { String ip -> println "-- Coach $ip" }
    }

    void echo(Object s) {
        println s
    }

    void initManagerFile() {
        String members = ""
        privateIps.each { String memberIp -> members += "<address>$memberIp:$COACH_PORT</address>\n" }

        "cp manager-hazelcast-template.xml manager-hazelcast.xml".execute()
        "cat manager-hazelcast.xml | sed -e \"s:MEMBERS:$members:\" > manager-hazelcast.xml.bak && mv manager-hazelcast.xml.bak manager-hazelcast.xml".execute()
    }

    void initPrivateIps(List<String> ids) {
        def x = "ec2-describe-instances".execute().text
        x.eachLine { String line, count ->
            def columns = line.split()
            if ("INSTANCE" == columns[0]) {
                for (int k = 0; k < columns.length; k++) {
                    println "$k " + columns[k]
                }

                def id = columns[1]
                if (ids.contains(id)) {
                    //14 is private ip
                    //privateIps << columns[14]
                    //3 is public dns
                    privateIps << columns[3]
                }
            }
        }

        echo "Private Ips: $privateIps"

        machineListFile.text = ""
        privateIps.each { String ip ->
            machineListFile.text += "$ip\n"
        }
    }

    void awaitStartup(List<String> ids) {
        def remainingIds = ids.clone()

        echo "RemainingIds: $remainingIds"

        for (int k = 1; k < 600; k++) {
            def lines = "ec2-describe-instance-status".execute().text.split("\n")
            echo "Status scan $k"
            for (int l = 0; l < (lines.length / 3); l++) {
                def instanceLine = lines[l * 3].split()
                def systemStatusLine = lines[l * 3 + 1].split()
                def instanceStatusLine = lines[l * 3 + 2].split()

                def id = instanceLine[1]
                if (remainingIds.contains(id)) {
                    def status = instanceStatusLine[2]
                    def started = status == "passed"
                    if (started) {
                        remainingIds.remove(id)
                    } else {
                        println "    $id $status"
                    }
                }

                if (remainingIds.size == 0) return
            }
        }

        echo "Timeout waiting for all instances to start, failed instances:"
        echo remainingIds
        System.exit(1)
    }
}


def cluster = new Cluster()
cluster.spawnMachines()
cluster.initManagerFile()
cluster.installCoaches()
cluster.startCoaches()