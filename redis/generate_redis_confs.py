#!/usr/bin/env python3
"""
Generate Redis configuration files for multiple instances.
All data is stored in the script's directory.
"""

from pathlib import Path

# ========== 可修改区域 ==========
INSTANCE_COUNT = 128
# 使用脚本所在目录作为基础目录
BASE_DIR = Path(__file__).parent / 'data'
TEMPLATE_CONF = Path(__file__).parent / 'base_redis.conf'
# BASE_PORT = 6380
# ===============================

conf_dir = BASE_DIR / 'confs'
pid_dir = BASE_DIR / 'pids'
log_dir = BASE_DIR / 'logs'
data_dir = BASE_DIR / 'datas'

# 创建基础目录
for d in [conf_dir, pid_dir, log_dir, data_dir]:
    d.mkdir(parents=True, exist_ok=True)

# 检查模板文件是否存在
if not TEMPLATE_CONF.exists():
    print(f"❌ Error: Template file not found at {TEMPLATE_CONF}")
    exit(1)

with open(TEMPLATE_CONF, 'r') as f:
    template = f.read()

for i in range(INSTANCE_COUNT):
    # port = BASE_PORT + i
    pid_path = pid_dir / f'redis_{i}.pid'
    log_path = log_dir / f'redis_{i}.log'
    data_path = data_dir / f'redis_{i}'
    conf_path = conf_dir / f'redis_{i}.conf'

    data_path.mkdir(parents=True, exist_ok=True)

    config = template.replace(
        '{id}',
        str(i)
    ).replace(
        'pidfile 0',
        f'pidfile {pid_path}'
    ).replace(
        'logfile 0',
        f'logfile {log_path}'
    ).replace('dir 0',
        f'dir {data_path}'
    )

    with open(conf_path, 'w') as f:
        f.write(config)

print(f'✅ Successfully generated {INSTANCE_COUNT} Redis config files in {conf_dir}')
print(f'📁 Data directory: {BASE_DIR}')
