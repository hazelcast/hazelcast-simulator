#!/bin/bash

# exit on failure
set -e

 # Check if python is installed.
if ! hash aws 2>/dev/null ; then
    echo "[ERROR]awscli is not installed!"
    echo "[ERROR]On OSX type: 'brew install awscli'"
    echo "[ERROR]On Ubuntu type 'sudo apt-get install awscli'"
    echo "[ERROR]On RHEL/Fedora type 'sudo yum install awscli'"
    exit 1
fi

# todo:
# - security-group: automatic create
# - init script
# --ebs-optimized
# - INSTANCE_STORAGE_MKFS_OPTIONS
# - INSTANCE_STORAGE_MOUNT_OPTIONS
# - EC2_SPOT_PRICE
# - software installation
# - correctly handling killing non existing agents
#   'An error occurred (InvalidParameterCombination) when calling the TerminateInstances operation: No instances specified
#
# done
# - await for machine 'startup' doesn't timeout

count=$1

export AWS_ACCESS_KEY_ID=$CLOUD_IDENTITY
export AWS_SECRET_ACCESS_KEY=$CLOUD_CREDENTIAL

# will contain a comma separated list of instance_id's
instance_ids=""
security_group_id=""
public_ips=""

init_security_group(){
    echo "[INFO]Skipping security group configuration"
#    set +e
#    output=$(aws ec2 describe-security-groups \
#        --region $region \
#        --group-names x \
#        --query 'SecurityGroups[*].GroupId')
#    exit_status=$?
#    set -e
#
#    echo [INFO]$output
#    security_group_ids=""
#    for security_group_id in $output
#    do
#        instance_ids="$security_group_id,$security_group_ids"
#    done
#
#    echo "[INFO]$security_group_ids"
}

load_ids_rsa_pub(){
    if [ ! -f ~/.ssh/id_rsa.pub ]; then
        echo "[ERROR]Public key ~/.ssh/id_rsa.pub not found."
        echo "[ERROR]To create a public/private key pair execute [ssh-keygen -t rsa -C \"your_email@example.com\"]"
        echo "[ERROR]Hit enter for the password."
        exit 1
    fi

    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "[ERROR]Public key ~/.ssh/id_rsa.pub was found but private key  ~/.ssh/ida_rsa is missing."
        echo "[ERROR]To create a public/private key pair execute [ssh-keygen -t rsa -C \"your_email@example.com\"]"
        echo "[ERROR]Hit enter for the password."
        exit 1
    fi

    cat ~/.ssh/id_rsa.pub
}

start_instances(){
    id_rsa_pub=$(load_ids_rsa_pub)

    temp_file=$(mktemp $script.sh.XXXXXX)

    cat >$temp_file <<EOL
#!/bin/bash
exec > ~/init.out
exec 2> ~/init.err

# fix the sudoers file. inspired by org.jclouds.scriptbuilder.statements.login.Sudoers
rm -fr /etc/sudoers
echo "Defaults    secure_path=\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\" " >> /etc/sudoers
echo "root ALL = (ALL) ALL" >> /etc/sudoers
echo "%wheel ALL = (ALL) NOPASSWD:ALL" >> /etc/sudoers
chmod 0440 /etc/sudoers

# create the user
useradd $SIMULATOR_USER

# add the user to the sudo group
sudo usermod -a -G wheel $SIMULATOR_USER

# setup the ssh part
# unfortunately we can't sudo to the user and create the directories the normal way
mkdir -p /home/$SIMULATOR_USER/.ssh
chmod 700 /home/$SIMULATOR_USER/.ssh
touch /home/$SIMULATOR_USER/.ssh/authorized_keys
chmod 600 /home/$SIMULATOR_USER/.ssh/authorized_keys
echo $id_rsa_pub>>/home/$SIMULATOR_USER/.ssh/authorized_keys
chown -R $SIMULATOR_USER /home/$SIMULATOR_USER/
EOL
    args=""
    args="$args --image-id $IMAGE_ID"
    args="$args --region $REGION"
    args="$args --count $count"
    args="$args --instance-type $INSTANCE_TYPE"
    args="$args --instance-initiated-shutdown-behavior terminate"

    if [ "$SUBNET_ID" = "default" ]; then
        args="$args --security-groups $SECURITY_GROUP"
    else
        args="$args --subnet $SUBNET_ID"
    fi

    if [ "$PLACEMENT_GROUP" != "default" ]; then
        args="$args --placement GroupName=$PLACEMENT_GROUP"
    fi


    # the following seems to work on c3.4xlarge
    #  --block-device-mappings '[{"DeviceName":"/dev/sdb","VirtualName":"ephemeral0"},{"DeviceName":"/dev/sdc","VirtualName":"ephemeral1"}]' \
    # comment regarding missnig mappings is fine: https://eucalyptus.atlassian.net/browse/EUCA-9148
#        --block-device-mappings '[{"DeviceName": "/dev/sdf", "VirtualName": "ephemeral0" }, {"DeviceName": "/dev/sdg", "VirtualName": "ephemeral1" }, {"DeviceName": "/dev/sdh", "VirtualName": "ephemeral2" }, {"DeviceName": "/dev/sdi", "VirtualName": "ephemeral3" }, {"DeviceName": "/dev/sdj", "VirtualName": "ephemeral4" }, {"DeviceName": "/dev/sdk", "VirtualName": "ephemeral5" }, {"DeviceName": "/dev/sdl", "VirtualName": "ephemeral6" }, {"DeviceName": "/dev/sdm", "VirtualName": "ephemeral7" }, {"DeviceName": "/dev/sdn", "VirtualName": "ephemeral8" }, {"DeviceName": "/dev/sdo", "VirtualName": "ephemeral9" } ]' \

    output=$(aws ec2 run-instances \
        $args \
        --user-data file://$temp_file \
        --output text \
        --query 'Instances[*].InstanceId')

    for instance_id in $output
    do
        instance_ids="$instance_id,$instance_ids"
    done
}

set_instances_name(){
    for instance_id in ${instance_ids//,/ } ; do
        dt=$(date '+%d_%m_%Y__%H_%M_%S')
        name="$GROUP_NAME-$dt"
        echo "[INFO]$name"
        aws ec2 create-tags \
            --region $REGION \
            --resources $instance_id \
            --tags Key=Name,Value=$name
    done
}

await_instances_completion(){
    for instance_id in ${instance_ids//,/ } ; do

        # wait for the instance to be started
        for i in `seq 1 600`;
        do
            state=$(aws ec2 describe-instances \
                --region $REGION \
                --instance-ids $instance_id \
                --output text \
                --query 'Reservations[*].Instances[*].State.Name')

            if [ "$state" != "pending" ] ; then
                break
            fi

            if [ "$i" -gt "60" ] ; then
                echo "[WARN]Agent instance_id=$instance_id state=$state"
            fi
        done

        if [ "$state" == "running" ] ; then
            echo "Agent instance_id=$instance_id running"
        elif [ "$state" == "pending" ] ; then
            echo "[ERROR]Agent instance_id=$instance_id failed to start; it is still in 'pending' state"
            exit 1
        else
            echo "[ERROR]Agent instance_id=$instance_id failed to start; state [$state]"
            exit 1
        fi
    done
}

list_ips(){
    for instance_id in ${instance_ids//,/ } ; do
        public_ip=$(aws ec2 describe-instances \
                --region $REGION \
                --instance-ids $instance_id \
                --output text \
                --query 'Reservations[*].Instances[*].PublicIpAddress')
        private_ip=$(aws ec2 describe-instances \
                --region $REGION \
                --instance-ids $instance_id \
                --output text \
                --query 'Reservations[*].Instances[*].PrivateIpAddress')
        public_ips="$public_ips $public_ip"
        echo [INFO]$public_ip $private_ip
        echo "$public_ip,$private_ip" >> agents.txt
    done
}

post_init(){
    echo "[INFO]Waiting for agent-machines to come online..."
    for public_ip in ${public_ips//,/ } ; do
        ok=0
        for i in `seq 1 60`;
        do
            set +e
            ssh $SSH_OPTIONS -o ConnectTimeout=1 -q $SIMULATOR_USER@$public_ip exit
            status=$?
            set -e

            if [ "$status" -eq "0" ]; then
                echo "[INFO]    Agent machine $public_ip online"
                ok=1
                break
            fi
            sleep 2
        done

        if [ "$ok" -eq "0" ]; then
            echo "[ERROR]Failed to connect to agent $public_ip"
            exit 1
        fi
    done
    echo "[INFO]All agents-machines are online!"
}

init_security_group
start_instances
set_instances_name
await_instances_completion
list_ips
post_init