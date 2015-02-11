Manual Machine Setup
===========================

1. Copy the ```STABILIZER_HOME/conf/stabilizer.properties``` to your working directory.

2. Change the "USER" in the ```stabilizer.properties``` , if you want to use different user name than 'stabilizer'.
    
3. Create a RSA key pair or use an existing one. The usage of the key should not require to enter the pass-phrase manually. A key with pass-phrase and ssh-agent-forwarding is strongly recommended, but a pass-phrase-less key will also work.
    
4. Copy the public key into the ```~/.ssh/authorized_keys``` file on the remote machines.

5. Create an ```agents.txt``` file and add the ip addresses of the machines.

6. Call ```provisioner --restart ``` to verify.


