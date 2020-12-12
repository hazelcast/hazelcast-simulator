FROM ubuntu:20.04

RUN apt-get update && apt-get install -y openssh-server rsync sudo net-tools vim openjdk-14-jdk-headless
RUN mkdir /var/run/sshd
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
