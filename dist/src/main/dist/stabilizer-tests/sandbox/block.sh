#!/bin/sh

INF=$1

echo "Blocking interface $INF"

sudo iptables -A INPUT -p tcp --dport 22 -m state --state NEW,ESTABLISHED -j ACCEPT
sudo iptables -A OUTPUT -p tcp --sport 22 -m state --state ESTABLISHED -j ACCEPT

sudo iptables -A INPUT -p tcp --dport 9000 -m state --state NEW,ESTABLISHED -j ACCEPT
sudo iptables -A OUTPUT -p tcp --sport 9001 -m state --state ESTABLISHED -j ACCEPT

sudo iptables -A INPUT -p tcp --dport 9000 -m state --state NEW,ESTABLISHED -j ACCEPT
sudo iptables -A OUTPUT -p tcp --sport 9001 -m state --state ESTABLISHED -j ACCEPT

sudo iptables -A INPUT -i $INF -j DROP
sudo iptables -A OUTPUT -o $INF -j DROP