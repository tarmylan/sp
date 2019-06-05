说明：Ubuntu 18.04前几天发布了，改变挺大的，内核也直接升到了正式版4.15，而BBR内核要求为4.9，也就是说满足了，所以我们不需要换内核就可以很快的开启BBR，这里简单说下方法。

提示：Ubuntu 18.10依然可以用该方法开启BBR，而对于19.04、貌似官方直接给你开启了BBR，不需要重复开启了。
方法
1、修改系统变量

echo "net.core.default_qdisc=fq" >> /etc/sysctl.conf
echo "net.ipv4.tcp_congestion_control=bbr" >> /etc/sysctl.conf
2、保存生效

sysctl -p
3、查看内核是否已开启BBR

sysctl net.ipv4.tcp_available_congestion_control
显示以下即已开启：

# sysctl net.ipv4.tcp_available_congestion_control
net.ipv4.tcp_available_congestion_control = bbr cubic reno
4、查看BBR是否启动

lsmod | grep bbr
显示以下即启动成功：

# lsmod | grep bbr
tcp_bbr                20480  14
