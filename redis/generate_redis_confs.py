import os

# ========== 可修改区域 ==========
INSTANCE_COUNT = 1024
BASE_DIR = os.path.expanduser("~/data/SERVERS/redis")
TEMPLATE_CONF = os.path.join(BASE_DIR, "base_redis.conf")
BASE_PORT = 6380
# ===============================

conf_dir = os.path.join(BASE_DIR, "confs")
# sock_dir = os.path.join(BASE_DIR, "sockets")
pid_dir = os.path.join(BASE_DIR, "pids")
log_dir = os.path.join(BASE_DIR, "logs")
data_dir = os.path.join(BASE_DIR, "datas")

# 创建基础目录
for d in [conf_dir, pid_dir, log_dir, data_dir]:
    os.makedirs(d, exist_ok=True)

# for d in [conf_dir, sock_dir, pid_dir, log_dir, data_dir]:
#     os.makedirs(d, exist_ok=True)

with open(TEMPLATE_CONF, 'r') as f:
    template = f.read()

for i in range(INSTANCE_COUNT):
    # sock_path = os.path.join(sock_dir, f"redis_{i}.sock")
    port = BASE_PORT + i
    pid_path = os.path.join(pid_dir, f"redis_{i}.pid")
    log_path = os.path.join(log_dir, f"redis_{i}.log")
    data_path = os.path.join(data_dir, f"redis_{i}")
    conf_path = os.path.join(conf_dir, f"redis_{i}.conf")

    os.makedirs(data_path, exist_ok=True)

    config = template.replace(
        "port 0",
        f"port {port}"
    ).replace(
        "pidfile 0",
        f"pidfile {pid_path}"
    ).replace(
        "logfile 0",
        f"logfile {log_path}"
    ).replace("dir 0",
        f"dir {data_path}"
    )
    # .replace(
    #     "unixsocket 0",
    #     f"unixsocket {sock_path}"
    # )

    with open(conf_path, 'w') as f:
            f.write(config)

    # print(f"Generated config: {conf_path}")

print(f"\nSuccessfully generated {INSTANCE_COUNT} Redis config files in {conf_dir}")
