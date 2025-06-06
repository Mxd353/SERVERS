# arp_module.py
from scapy.all import ARP, Ether, srp

def get_mac(ip):
    arp_packet = Ether(dst="ff:ff:ff:ff:ff:ff")/ARP(pdst=ip)
    try:
        answered = srp(arp_packet, iface="ens13f0", timeout=2, verbose=False)[0]
        return answered[0][1].hwsrc if answered else ""
    except Exception as e:
        print(f"ARP请求失败: {e}")
        return ""
