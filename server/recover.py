#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
recover.py: скачивает видео по прямой ссылке и пытается собрать из него рабочий файл,
даже если часть файла на сервере битая (сервер зависает или отдаёт ошибку).

Как это работает
  1. Узнаёт размер файла (HEAD, затем Range "bytes=-1", затем параметр fsize в ссылке).
     Запросы идут с конца, потому что начало файла может быть битым.
  2. Качает файл блоками по 256 КБ, у каждого свой короткий таймаут.
     Блок не прочитался: делит его на куски по 16 КБ и находит, где именно битое место.
     Битые куски заполняются нулями, чтобы все смещения в файле остались на местах.
  3. Прогресс сохраняется в *.state.json. Скрипт можно прервать (Ctrl+C) и запустить снова,
     при просроченной ссылке (HTTP 403/404/410) вставьте новую, докачка продолжится.
  4. Если потеряно начало файла (ftyp и заголовок mdat), а служебный блок moov
     в конце цел, скрипт пересобирает начало, и все смещения остаются верными.
  5. ffmpeg переупаковывает файл, игнорируя ошибки, а при неудаче перекодирует в H.264.

Только стандартная библиотека Python 3.8+. Для финальной сборки нужен ffmpeg в PATH.

Примеры
  python recover.py                      (ссылку спросит сам: так не нужны кавычки)
  python recover.py "https://..." -o video.mp4
  python recover.py --refine             (второй проход: найти читаемые острова внутри битых зон)
  python recover.py --refine --stride 16384 --probe-timeout 30   (гуще сетка, терпеливее)
  python recover.py --probe 7000000 --probe-timeout 180          (зависло насовсем или просто медленно?)
  python recover.py --retry-bad          (ещё раз попытаться прочитать битые участки)
  python recover.py --skip-download      (только повторить сборку из уже скачанного)
  python recover.py --reencode           (сразу перекодировать в H.264 для браузера)
"""

import argparse
import http.client
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qs, urlparse

UA = "Mozilla/5.0 (recover.py)"
MB = 1024 * 1024


class FatalHTTP(Exception):
    """Ссылка недоступна целиком (просрочена, удалена) или сервер не умеет Range."""


class BlockFail(Exception):
    """Один блок не удалось прочитать (таймаут, 5xx, обрыв)."""


# ---------------------------------------------------------------- диапазоны [start, end)

def merge(ranges):
    out = []
    for s, e in sorted((int(a), int(b)) for a, b in ranges):
        if s >= e:
            continue
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    return out


def subtract(total, *sets):
    cover = merge([r for rs in sets for r in rs])
    res, pos = [], 0
    for s, e in cover:
        if s > pos and pos < total:
            res.append([pos, min(s, total)])
        pos = max(pos, e)
    if pos < total:
        res.append([pos, total])
    return [r for r in res if r[0] < r[1]]


def overlaps(ranges, s, e):
    return any(a < e and s < b for a, b in ranges)


# ---------------------------------------------------------------- HTTP

def request(url, headers=None, method="GET", timeout=20):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h, method=method)
    return urllib.request.urlopen(req, timeout=timeout)


def fetch_range(url, start, end, timeout):
    """Байты [start, end). Бросает BlockFail или FatalHTTP."""
    want = end - start
    deadline = timeout + want / 20000.0  # не медленнее ~20 КБ/с
    t0 = time.monotonic()
    try:
        resp = request(url, {"Range": "bytes=%d-%d" % (start, end - 1)}, timeout=timeout)
    except urllib.error.HTTPError as e:
        if e.code in (401, 403, 404, 410):
            raise FatalHTTP("HTTP %d" % e.code)
        raise BlockFail("HTTP %d" % e.code)
    except (OSError, http.client.HTTPException) as e:
        raise BlockFail(str(e))

    try:
        if resp.status != 206:
            raise FatalHTTP("сервер не поддерживает Range (ответил HTTP %d)" % resp.status)
        buf = bytearray()
        while len(buf) < want:
            if time.monotonic() - t0 > deadline:
                raise BlockFail("слишком медленно")
            chunk = resp.read(min(65536, want - len(buf)))
            if not chunk:
                break
            buf += chunk
    except (OSError, http.client.HTTPException) as e:
        raise BlockFail(str(e))
    finally:
        try:
            resp.close()
        except Exception:
            pass

    if len(buf) != want:
        raise BlockFail("получено %d из %d байт" % (len(buf), want))
    return bytes(buf)


STOP = threading.Event()  # выставляется при Ctrl+C или фатальной ошибке, чтобы потоки быстро завершились


def try_get(url, s, e, timeout, retries):
    for _ in range(max(1, retries)):
        if STOP.is_set():
            return None
        try:
            return fetch_range(url, s, e, timeout)
        except BlockFail:
            pass
    return None


def fetch_block(url, s, e, args):
    """
    Для параллельного режима (выполняется в потоке). Возвращает ([(смещение, байты)], [(начало, конец)]):
    что прочиталось и что нет. Сначала быстрая проба начала блока: если она не ответила,
    весь блок считается битым без долгих попыток (границы потом уточнит --refine).
    """
    if try_get(url, s, min(e, s + args.sub), args.sub_timeout, 1) is None:
        return [], [(s, e)]
    data = try_get(url, s, e, args.timeout, args.retries)
    if data is not None:
        return [(s, data)], []
    oks, bads = [], []
    q = s
    while q < e:
        r = min(e, q + args.sub)
        d = try_get(url, q, r, args.sub_timeout, 1)
        if d is None:
            bads.append((q, r))
        else:
            oks.append((q, d))
        q = r
    return oks, bads


def detect_size(url, timeout, forced):
    if forced:
        return forced
    size = None
    try:  # 1. HEAD
        r = request(url, method="HEAD", timeout=timeout)
        cl = r.headers.get("Content-Length")
        r.close()
        if cl and cl.isdigit() and int(cl) > 0:
            size = int(cl)
    except Exception:
        pass
    if size is None:  # 2. суффиксный Range: берём последний байт, он точно не в битом начале
        try:
            r = request(url, {"Range": "bytes=-1"}, timeout=timeout)
            cr = r.headers.get("Content-Range", "")
            r.read(1)
            r.close()
            m = re.search(r"/(\d+)$", cr)
            if m:
                size = int(m.group(1))
        except Exception:
            pass
    q = parse_qs(urlparse(url).query).get("fsize")
    fsize = int(q[0]) if q and q[0].isdigit() else None
    if size is None:
        size = fsize  # 3. параметр fsize в ссылке (Яндекс Диск)
    elif fsize and fsize != size:
        print("! Размер по ответу сервера (%d) и fsize в ссылке (%d) различаются, беру ответ сервера." % (size, fsize))
    return size


# ---------------------------------------------------------------- состояние

def load_state(path):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(path, st):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, path)


# ---------------------------------------------------------------- скачивание

def download(url, size, raw_path, state_path, args):
    st = load_state(state_path)
    ok = merge(st.get("ok", []))
    bad = merge(st.get("bad", []))
    if st and st.get("size") != size:
        print("! В сохранённом состоянии размер %s, а сейчас %d. Это другой файл: укажите другое имя через -o."
              % (st.get("size"), size))
        sys.exit(2)
    if not os.path.exists(raw_path):
        open(raw_path, "wb").close()
        ok, bad = [], []
    if args.retry_bad:
        bad = []

    f = open(raw_path, "r+b")
    f.truncate(size)  # недокачанное место читается как нули

    def flush_state():
        save_state(state_path, {"size": size, "ok": merge(ok), "bad": merge(bad)})

    def note(msg):
        sys.stdout.write("\r%s%s\n" % (msg, " " * 20))
        sys.stdout.flush()

    def progress(pos):
        got = sum(e - s for s, e in ok)
        lost = sum(e - s for s, e in bad)
        sys.stdout.write("\r  %5.1f%%  получено %.1f МБ, потеряно %.0f КБ, позиция %.1f МБ   "
                         % (100.0 * (got + lost) / size, got / MB, lost / 1024.0, pos / MB))
        sys.stdout.flush()

    def put(s, data):
        f.seek(s)
        f.write(data)
        ok.append([s, s + len(data)])

    def lose(s, e):
        bad.append([s, e])

    todo = subtract(size, ok, bad)
    total_todo = sum(e - s for s, e in todo)
    if total_todo == 0:
        print("Всё уже обработано в прошлый раз (для повторной попытки на битых местах: --retry-bad).")
        f.close()
        return
    print("Размер файла: %d байт (%.1f МБ). К загрузке: %.1f МБ." % (size, size / MB, total_todo / MB))

    if args.jobs > 1:
        # Параллельный режим: мёртвые блоки не блокируют остальные.
        pool = ThreadPoolExecutor(max_workers=args.jobs)
        futs = {}
        try:
            for rs, re_ in todo:
                p = rs
                while p < re_:
                    q = min(re_, p + args.block)
                    futs[pool.submit(fetch_block, url, p, q, args)] = (p, q)
                    p = q
            done = 0
            for fu in as_completed(futs):
                oks, bads = fu.result()
                for off, data in oks:
                    put(off, data)
                for s_, e_ in bads:
                    lose(s_, e_)
                done += 1
                if done % 25 == 0:
                    flush_state()
                progress(futs[fu][1])
            pool.shutdown(wait=True)
        except FatalHTTP as ex:
            STOP.set()
            for fu in futs:
                fu.cancel()
            pool.shutdown(wait=False)
            flush_state()
            note("\n! Остановка: %s. Скорее всего, ссылка просрочена или удалена.\n"
                 "  Прогресс сохранён: возьмите новую ссылку и запустите скрипт снова с тем же -o." % ex)
            f.close()
            sys.exit(3)
        except KeyboardInterrupt:
            STOP.set()
            for fu in futs:
                fu.cancel()
            pool.shutdown(wait=False)
            flush_state()
            note("\n! Прервано. Прогресс сохранён, запустите снова, чтобы продолжить.")
            f.close()
            sys.exit(130)
        f.close()
        flush_state()
        progress(size)
        print()
        return

    dead = 0  # сколько блоков подряд не прочиталось целиком
    try:
        for rs, re_ in todo:
            p = rs
            while p < re_:
                s, e = p, min(re_, p + args.block)
                p = e
                progress(s)

                if dead >= args.dead_limit:
                    # режим «всё мёртвое»: быстро проверяем начало блока, чтобы не тратить часы
                    if try_get(url, s, min(e, s + args.sub), args.sub_timeout, 1) is None:
                        lose(s, e)
                        flush_state()
                        continue

                data = try_get(url, s, e, args.timeout, args.retries)
                if data is not None:
                    put(s, data)
                    dead = 0
                    flush_state()
                    continue

                # блок не прочитался: ищем битое место кусками поменьше
                note("  блок %d–%d не читается, проверяю по частям…" % (s, e - 1))
                any_ok = False
                q = s
                while q < e:
                    r = min(e, q + args.sub)
                    d = try_get(url, q, r, args.sub_timeout, 1)
                    if d is None:
                        lose(q, r)
                    else:
                        put(q, d)
                        any_ok = True
                    q = r
                    progress(q)
                dead = 0 if any_ok else dead + 1
                if dead == args.dead_limit:
                    note("  %d блоков подряд не читаются целиком, дальше проверяю быстро "
                         "(потом можно уточнить: --retry-bad --dead-limit 999)." % dead)
                flush_state()
    except FatalHTTP as ex:
        flush_state()
        note("\n! Остановка: %s. Скорее всего, ссылка просрочена или удалена.\n"
             "  Прогресс сохранён: возьмите новую ссылку и запустите скрипт снова с тем же -o." % ex)
        f.close()
        sys.exit(3)
    except KeyboardInterrupt:
        flush_state()
        note("\n! Прервано. Прогресс сохранён, запустите снова, чтобы продолжить.")
        f.close()
        sys.exit(130)

    f.close()
    flush_state()
    progress(size)
    print()


# ---------------------------------------------------------------- уточнение битых зон

def probe_one(url, size, off, args):
    """Одна проба с выводом времени: помогает понять, сервер завис насовсем или просто медленный."""
    n = args.probe_size
    t0 = time.monotonic()
    try:
        d = fetch_range(url, off, min(size, off + n), args.probe_timeout)
        print("Читается: %d байт по смещению %d за %.1f с." % (len(d), off, time.monotonic() - t0))
        print("Нулей в прочитанном: %.0f%%" % (100.0 * d.count(0) / max(1, len(d))))
    except BlockFail as ex:
        print("Не прочиталось за %.1f с (%s)." % (time.monotonic() - t0, ex))
        print("Если это место важно, повторите с большим таймаутом: --probe %d --probe-timeout 180" % off)
    except FatalHTTP as ex:
        print("Ссылка недоступна: %s" % ex)


def refine(url, size, state_path, args):
    """
    Внутри зон, помеченных битыми, ищет читаемые «острова»:
      1. частая сетка проб (параллельно: зависшие запросы не тормозят остальные);
      2. на границе «читается / не читается» бинарным поиском находит точный край;
      3. найденные острова возвращаются в очередь, и обычная загрузка их скачивает.
    """
    st = load_state(state_path)
    ok = merge(st.get("ok", []))
    bad = merge(st.get("bad", []))
    if not bad:
        print("Битых участков нет, уточнять нечего.")
        return
    P = max(512, args.probe_size)
    gran = max(512, args.gran)

    def test(x):
        x = max(0, min(x, size - 1))
        return try_get(url, x, min(size, x + P), args.probe_timeout, 1) is not None

    # --- 1. сетка проб
    points = []
    for a, b in bad:
        x = a
        while x < b:
            points.append((a, b, min(x, max(a, b - P))))
            x += args.stride
        tail = max(a, b - P)  # хвост зоны короче шага тоже нужно проверить
        if (a, b, tail) not in points:
            points.append((a, b, tail))
    print("Битых зон: %d. Проб сеткой: %d (шаг %d КБ, параллельно %d)…"
          % (len(bad), len(points), args.stride // 1024, args.jobs))

    results = {}
    pool = ThreadPoolExecutor(max_workers=max(1, args.jobs))
    futs = {pool.submit(test, px): px for _, _, px in points}
    try:
        for n, fu in enumerate(as_completed(futs), 1):
            results[futs[fu]] = fu.result()
            sys.stdout.write("\r  проверено %d из %d, читаемых проб: %d   "
                             % (n, len(futs), sum(1 for v in results.values() if v)))
            sys.stdout.flush()
    except FatalHTTP as ex:
        STOP.set()
        for fu in futs:
            fu.cancel()
        pool.shutdown(wait=False)
        print("\n! Остановка: %s. Ссылка просрочена или удалена: возьмите новую и запустите снова." % ex)
        sys.exit(3)
    except KeyboardInterrupt:
        STOP.set()
        for fu in futs:
            fu.cancel()
        pool.shutdown(wait=False)
        print("\n! Прервано. Состояние не менялось.")
        sys.exit(130)
    pool.shutdown(wait=True)
    print()

    # --- 2. границы островов бинарным поиском
    islands = []
    try:
        for a, b in bad:
            pts = sorted(px for px in results if a <= px < b)
            i = 0
            while i < len(pts):
                if not results[pts[i]]:
                    i += 1
                    continue
                j = i
                while j + 1 < len(pts) and results[pts[j + 1]]:
                    j += 1
                if i == 0:
                    left = a
                else:
                    lo, hi = pts[i - 1], pts[i]          # lo не читается, hi читается
                    while hi - lo > gran:
                        mid = (lo + hi) // 2
                        if test(mid):
                            hi = mid
                        else:
                            lo = mid
                    left = hi
                if j == len(pts) - 1:
                    right = b
                else:
                    lo, hi = pts[j], pts[j + 1]          # lo читается, hi не читается
                    while hi - lo > gran:
                        mid = (lo + hi) // 2
                        if test(mid):
                            lo = mid
                        else:
                            hi = mid
                    right = min(b, lo + P)
                if right > left:
                    islands.append([left, right])
                i = j + 1
    except FatalHTTP as ex:
        print("! Остановка: %s. Ссылка просрочена." % ex)
        sys.exit(3)
    except KeyboardInterrupt:
        print("\n! Прервано. Состояние не менялось.")
        sys.exit(130)

    islands = merge(islands)
    if not islands:
        print("Читаемых мест внутри битых зон не нашлось.\n"
              "Если сервер просто медленный, а не зависший, увеличьте --probe-timeout (например, 60)\n"
              "или проверьте одно место: --probe СМЕЩЕНИЕ --probe-timeout 180")
        return
    back = sum(e - s for s, e in islands)
    print("Найдено читаемых островов: %d, всего %.2f МБ. Возвращаю их в очередь на загрузку." % (len(islands), back / MB))
    for s, e in islands[:20]:
        print("   %d – %d  (%.0f КБ)" % (s, e - 1, (e - s) / 1024.0))
    if len(islands) > 20:
        print("   … и ещё %d" % (len(islands) - 20))

    new_bad = subtract(size, islands, subtract(size, bad))  # bad минус острова
    save_state(state_path, {"size": size, "ok": ok, "bad": new_bad})


# ---------------------------------------------------------------- анализ MP4 и ремонт начала

def box(typ, payload=b""):
    return struct.pack(">I", 8 + len(payload)) + typ + payload


def find_moov(path, size):
    """Ищет последний правдоподобный блок moov (с вложенным mvhd). Возвращает (start, size) или None."""
    found = {}
    chunk = 4 * MB
    with open(path, "rb") as f:
        pos, tail = 0, b""
        while pos < size:
            f.seek(pos)
            data = f.read(chunk)
            if not data:
                break
            buf = tail + data
            base = pos - len(tail)
            i = 0
            while True:
                j = buf.find(b"moov", i)
                if j < 0:
                    break
                i = j + 1
                if j < 4 or j + 12 > len(buf):
                    continue
                if buf[j + 8:j + 12] != b"mvhd":
                    continue
                bsize = struct.unpack(">I", buf[j - 4:j])[0]
                start = base + j - 4
                if bsize >= 100 and start + bsize <= size:
                    found[start] = bsize
            pos += len(data)
            tail = buf[-16:]
    if not found:
        return None
    start = max(found)
    return start, found[start]


def patch_head(raw_path, patched_path, head_end, moov_start):
    """
    Начало файла [0, head_end) потеряно (там нули). Пересобираем его так, чтобы все
    абсолютные смещения внутри moov остались верными:
        ftyp | free (до заголовка mdat) | mdat-заголовок | ...целые данные... | moov
    """
    ftyp = box(b"ftyp", b"isom" + struct.pack(">I", 512) + b"isomiso2avc1mp41")
    hdr_len = 8
    hdr_pos = head_end - hdr_len
    mdat_size = moov_start - hdr_pos
    if mdat_size > 0xFFFFFFFF:
        hdr_len = 16
        hdr_pos = head_end - hdr_len
        mdat_size = moov_start - hdr_pos
        hdr = struct.pack(">I", 1) + b"mdat" + struct.pack(">Q", mdat_size)
    else:
        hdr = struct.pack(">I", mdat_size) + b"mdat"
    free_size = hdr_pos - len(ftyp)
    if free_size < 8 or hdr_pos > 0xFFFFFFFF:
        return False
    shutil.copyfile(raw_path, patched_path)
    with open(patched_path, "r+b") as f:
        f.seek(0)
        f.write(ftyp)
        f.write(struct.pack(">I", free_size) + b"free")
        f.seek(hdr_pos)
        f.write(hdr)
    return True


# ---------------------------------------------------------------- ffmpeg

def run_ffmpeg(src, dst, reencode):
    ff = shutil.which("ffmpeg")
    if not ff:
        return False
    head = [ff, "-hide_banner", "-loglevel", "error", "-y", "-err_detect", "ignore_err"]
    maps = ["-map", "0:v?", "-map", "0:a?"]
    attempts = []
    if not reencode:
        attempts.append(("без перекодирования",
                         head + ["-fflags", "+genpts+discardcorrupt", "-i", src] + maps +
                         ["-c", "copy", "-movflags", "+faststart", dst]))
    # Перекодирование: декодер маскирует повреждённые кадры, и в результате получается
    # непрерывный поток, который плеер проигрывает до конца, не останавливаясь на дырах.
    attempts.append(("перекодирование в H.264/AAC",
                     head + ["-fflags", "+genpts", "-max_error_rate", "1.0", "-i", src] + maps +
                     ["-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p",
                      "-c:a", "aac", "-b:a", "160k", "-movflags", "+faststart", dst]))
    for name, cmd in attempts:
        print("ffmpeg: %s…" % name)
        if os.path.exists(dst):
            os.remove(dst)
        r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                           universal_newlines=True, errors="replace")
        if r.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) > 2048:
            print("  готово: %s (%.1f МБ)" % (dst, os.path.getsize(dst) / MB))
            return True
        tail = "\n".join(r.stderr.strip().splitlines()[-4:])
        print("  не вышло.%s" % (("\n  " + tail.replace("\n", "\n  ")) if tail else ""))
    return False


def postprocess(raw_path, state_path, base, out, args):
    st = load_state(state_path)
    if not st:
        print("Нет сохранённого состояния: сначала скачайте файл.")
        sys.exit(2)
    size = st["size"]
    bad = merge(st.get("bad", []))
    lost = sum(e - s for s, e in bad)

    print("\n=== Итог загрузки ===")
    print("Потеряно %.0f КБ из %.1f МБ (%.2f%%), битых участков: %d" %
          (lost / 1024.0, size / MB, 100.0 * lost / size, len(bad)))
    for s, e in bad[:30]:
        print("   %d – %d  (%.0f КБ)" % (s, e - 1, (e - s) / 1024.0))
    if len(bad) > 30:
        print("   … и ещё %d" % (len(bad) - 30))

    src = raw_path
    head_lost = bool(bad) and bad[0][0] == 0
    moov = find_moov(raw_path, size)
    moov_damaged = bool(moov) and overlaps(bad, moov[0], moov[0] + moov[1])

    print("\n=== Анализ MP4 ===")
    print("Начало файла (ftyp): %s" % ("потеряно" if head_lost else "цело"))
    if moov:
        print("Блок moov: найден на позиции %d, %d байт%s" %
              (moov[0], moov[1], ", ПОВРЕЖДЁН" if moov_damaged else ", цел"))
    else:
        print("Блок moov: не найден")

    if head_lost:
        head_end = bad[0][1]
        patched = base + ".patched.mp4"
        if moov and not moov_damaged and moov[0] > head_end and patch_head(raw_path, patched, head_end, moov[0]):
            print("Начало файла пересобрано: %s" % patched)
            src = patched
        else:
            print("Начало пересобрать не удалось: нужен целый блок moov после потерянного участка.")

    if not moov or moov_damaged:
        print("\n! Без целого moov видео собрать нельзя: в нём таблица, какие байты какой кадр.\n"
              "  Поможет утилита untrunc (github.com/anthwlock/untrunc) и рабочий файл с той же\n"
              "  камеры/телефона, записанный теми же настройками:\n"
              "      untrunc рабочий_файл.MP4 %s" % raw_path)

    if args.no_ffmpeg:
        print("\nСборку через ffmpeg пропустили (--no-ffmpeg). Файл для ручной работы: %s" % src)
        return
    print("\n=== Сборка ===")
    reencode = args.reencode
    if run_ffmpeg(src, out, reencode):
        if lost and not reencode:
            print("\nВ файле остались «дыры» на месте потерянных участков: плеер может застревать на первой из них.\n"
                  "Если так, соберите с перекодированием (декодер замажет дыры, поток станет непрерывным):\n"
                  "    python recover.py --skip-download --reencode")
        elif not reencode:
            print("Если файл не играет в браузере (часто из-за HEVC/H.265): python recover.py --skip-download --reencode")
    else:
        if shutil.which("ffmpeg") is None:
            print("ffmpeg не найден в PATH. Установите его (ffmpeg.org), затем: python recover.py --skip-download")
        else:
            print("ffmpeg не смог собрать файл. Промежуточный результат: %s\n"
                  "Его можно попробовать открыть в VLC или обработать через untrunc." % src)


# ---------------------------------------------------------------- main

def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser(description="Скачать и восстановить видеофайл по ссылке.")
    ap.add_argument("url", nargs="?", help="прямая ссылка (если не указана, скрипт спросит)")
    ap.add_argument("-o", "--out", default="recovered.mp4", help="итоговый файл (по умолчанию recovered.mp4)")
    ap.add_argument("--block", type=int, default=256 * 1024, help="размер блока, байт (256 КБ)")
    ap.add_argument("--sub", type=int, default=16 * 1024, help="размер куска при поиске битого места (16 КБ)")
    ap.add_argument("--timeout", type=float, default=20, help="таймаут блока, сек (20)")
    ap.add_argument("--sub-timeout", type=float, default=6, help="таймаут куска, сек (6)")
    ap.add_argument("--retries", type=int, default=2, help="попыток на целый блок (2)")
    ap.add_argument("--dead-limit", type=int, default=6, help="после стольких мёртвых блоков подряд проверять быстро (6)")
    ap.add_argument("--size", type=int, help="размер файла в байтах, если сервер его не сообщает")
    ap.add_argument("--retry-bad", action="store_true", help="снова попытаться прочитать ранее битые участки")
    ap.add_argument("--reencode", action="store_true", help="сразу перекодировать в H.264/AAC")
    ap.add_argument("--refine", action="store_true",
                    help="уточнить битые зоны: частая сетка проб + бинарный поиск границ читаемых островов")
    ap.add_argument("--stride", type=int, default=64 * 1024, help="шаг сетки проб в --refine, байт (64 КБ)")
    ap.add_argument("--probe-size", type=int, default=4096, help="размер одной пробы, байт (4096)")
    ap.add_argument("--probe-timeout", type=float, default=10, help="таймаут пробы, сек (10)")
    ap.add_argument("--gran", type=int, default=4096, help="точность поиска границ, байт (4096)")
    ap.add_argument("--jobs", type=int, default=12,
                    help="параллельных запросов при загрузке и в --refine (12). "
                         "--jobs 1: последовательный режим (там работает --dead-limit)")
    ap.add_argument("--probe", type=int, metavar="СМЕЩЕНИЕ",
                    help="прочитать одну пробу по смещению и показать время (проверка: сервер завис или просто медленный; "
                         "добавьте --probe-timeout 120)")
    ap.add_argument("--no-ffmpeg", action="store_true", help="только скачать, без сборки")
    ap.add_argument("--skip-download", action="store_true", help="не качать, только повторить сборку")
    args = ap.parse_args()

    base = os.path.splitext(args.out)[0]
    raw_path = base + ".raw.bin"
    state_path = base + ".state.json"

    if not args.skip_download:
        url = args.url
        if not url:
            url = input("Вставьте ссылку на видео: ")
        url = url.strip().strip("\"'")
        if not re.match(r"^https?://", url, re.I):
            print("Это не похоже на ссылку http(s).")
            sys.exit(2)

        size = detect_size(url, args.timeout, args.size)
        if not size:
            print("Не удалось узнать размер файла. Укажите его вручную: --size БАЙТ")
            sys.exit(2)
        if args.probe is not None:
            probe_one(url, size, args.probe, args)
            return
        if args.refine:
            if not os.path.exists(state_path):
                print("Сначала нужна обычная загрузка (без --refine): так появятся данные о битых зонах.")
                sys.exit(2)
            refine(url, size, state_path, args)
        download(url, size, raw_path, state_path, args)

    postprocess(raw_path, state_path, base, args.out, args)


if __name__ == "__main__":
    main()