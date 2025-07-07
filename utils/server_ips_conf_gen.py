def generate_ips():
    lines = []
    for i in range(7, 39):
        lines.append("# Rack {}".format(i - 7))
        for j in range(1, 33):
            lines.append("192.168.{}.{}".format(i, j))
        lines.append("")
    return lines

def main():
    ips = generate_ips()
    with open("server_ips.conf", "w") as f:
        for ip in ips:
            f.write(ip + "\n")
    print("Generated server_ips.conf with {} addresses.".format(len(ips)))


if __name__ == "__main__":
    main()
