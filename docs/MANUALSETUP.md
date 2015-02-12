Manual Machine Setup
===========================
1. Copy the ```STABILIZER_HOME/conf/stabilizer.properties``` to your working directory.

2. Change the "USER" in the ```stabilizer.properties``` , if you want to use different user name than 'stabilizer'.

3. Create a RSA key pair or use an existing one. The usage of the key should not require to enter the pass-phrase manually. A key with pass-phrase and ssh-agent-forwarding is strongly recommended, but a pass-phrase-less key will also work.
   
       You can check whether a key pair exists with this command:
       
          ls -al ~/.ssh
   
       If it does not exist, you can create key pair on the client machine with this command:
   
          ssh-keygen -t rsa
         
 > **You will get a few more questions:**
 >- Enter file in which to save the key (/home/demo/.ssh/id_rsa):
 >- Enter passphrase (empty for no passphrase):  **(It is optional)**
   
4. Copy the public key into the ```~/.ssh/authorized_keys``` file on the remote machines with this command:

        ssh-copy-id user@123.45.56.78
        

5. Create an ```agents.txt``` file and add the ip addresses of the machines. After the add ip adresses to your ```agents.txt``` file looks like:

        98.76.65.54
        10.28.37.46

6. Call ```provisioner --restart ``` to verify.

