#!/bin/bash

# exit on failure
set -e

 # Check if awscli is installed.
if ! hash aws 2>/dev/null ; then
    echo "[ERROR]awscli is not installed!"
    echo "[ERROR]On OSX type: 'brew install awscli'"
    echo "[ERROR]On Ubuntu type 'sudo apt-get install awscli'"
    echo "[ERROR]On RHEL/Fedora type 'sudo yum install awscli'"
    exit 1
fi

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

# enable this to fix the original account. Make sure the proper user for that
# account is used.
# echo $id_rsa_pub>>/home/ec2-user/.ssh/authorized_keys

exec > ~/init.out
exec 2> ~/init.err

addgroup wheel || true

# fix the sudoers file. inspired by org.jclouds.scriptbuilder.statements.login.Sudoers
rm -fr /etc/sudoers
echo "Defaults    env_reset" >> /etc/sudoers
echo "Defaults    secure_path=\"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\" " >> /etc/sudoers
echo "root ALL = (ALL) ALL" >> /etc/sudoers
echo "%wheel ALL = (ALL) NOPASSWD:ALL" >> /etc/sudoers
chmod 0440 /etc/sudoers

# create the user
useradd -m $SIMULATOR_USER -d /home/$SIMULATOR_USER/ -s /bin/bash

# add the user to the sudo group
sudo usermod -a -G wheel $SIMULATOR_USER

# setup the ssh part
# unfortunately we can't sudo to the user and create the directories the normal way
mkdir -p /home/$SIMULATOR_USER/.ssh
chmod 700 /home/$SIMULATOR_USER/.ssh
touch /home/$SIMULATOR_USER/.ssh/authorized_keys
chmod 600 /home/$SIMULATOR_USER/.ssh/authorized_keys
echo $id_rsa_pub>>/home/$SIMULATOR_USER/.ssh/authorized_keys
chown -R $SIMULATOR_USER:$SIMULATOR_USER /home/$SIMULATOR_USER/

# this is needed to fix problems due to SELinux
# https://blog.tinned-software.net/ssh-key-authentication-is-not-working-selinux/
sudo -H -u $SIMULATOR_USER bash -c "restorecon -r -v /home/$SIMULATOR_USER/" || true
EOL
    args=""
    args="$args --image-id $IMAGE_ID"
    args="$args --region $REGION"
    args="$args --count $count"
    args="$args --instance-type $INSTANCE_TYPE"
    args="$args --instance-initiated-shutdown-behavior terminate"

    if [ "$SUBNET_ID" = "default" ] || [ "$SUBNET_ID" = "" ]; then
        echo "[ERROR] Please specify a subnet ID via SUBNET_ID property."
        exit 1
    else
        args="$args --subnet $SUBNET_ID"
    fi

    if [ "$EC2_RUN_INSTANCE_ARGS" != "default" ]; then
        args="$args $EC2_RUN_INSTANCE_ARGS"
    elif [ "$PLACEMENT_GROUP" != "default" ]; then
        args="$args --placement GroupName=$PLACEMENT_GROUP"
    fi

    output=$(aws ec2 run-instances \
        $args \
        --user-data file://$temp_file \
        --output text \
        --query 'Instances[*].InstanceId')

    rm -fr $temp_file

    for instance_id in $output
    do
        instance_ids="$instance_id,$instance_ids"
    done

    # in some rare cases the created instance is not seen immediately.
    # so lets give them instances some time to start.
    # We would be waiting for instance_completion anyway.
    sleep 20
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
    echo "[INFO]Waiting for agent-machines to come online (can take minutes)..."
    for public_ip in ${public_ips//,/ } ; do
        ok=0
        for i in `seq 1 300`;
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

            if [ "$i" -ge "15" ]; then
                echo "[INFO]    Agent machine $public_ip still offline"
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