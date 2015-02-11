Manual Machine Setup
===========================

1. Copy the ```STABILIZER_HOME/conf/stabilizer.properties``` to your working directory.

2. Change the "USER" in the ```stabilizer.properties``` , if you want to use different user name than 'stabilizer'.
    
3. Create an password-less rsa key pair , if you don't have one.    
    
4. Copy the public key into the ```~/.ssh/authorized_keys``` file on the remote machines.

5. Create an ```agents.txt``` file and add the ip addresses of the machines.

6. Call ```provisioner --restart ``` to verify.


