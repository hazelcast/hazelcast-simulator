FROM ubuntu:20.04

RUN apt-get update && apt-get install -y openssh-server rsync sudo net-tools vim
RUN mkdir /var/run/sshd

# SSH login fix. Otherwise user is kicked off after login
#RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

RUN adduser --gecos "" simulator --disabled-password
RUN usermod -a -G sudo simulator

RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN mkdir /home/simulator/.ssh/
ADD id_rsa.pub /home/simulator/.ssh/authorized_keys
RUN chown simulator:simulator -R /home/simulator/.ssh

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
EXPOSE 9000
CMD ["/usr/sbin/sshd", "-D"]
