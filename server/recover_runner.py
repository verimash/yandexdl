#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запускает recover.py без изменений, но так, чтобы он ходил в сеть напрямую: без системных/переменных
прокси (на время перехвата туда прописан mitmproxy, а Python не доверяет его сертификату).

Использование (его вызывает server.py):  python recover_runner.py <аргументы recover.py>
Переменная YDISK_INSECURE_SSL=1 отключает проверку сертификатов, как и у загрузчика фото.
"""
import os
import runpy
import ssl
import sys
import urllib.request

handlers = [urllib.request.ProxyHandler({})]
if os.environ.get("YDISK_INSECURE_SSL"):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    handlers.append(urllib.request.HTTPSHandler(context=ctx))
urllib.request.install_opener(urllib.request.build_opener(*handlers))

script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recover.py")
sys.argv = [script] + sys.argv[1:]
runpy.run_path(script, run_name="__main__")