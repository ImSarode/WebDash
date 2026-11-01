#!/usr/bin/env python3

import argparse, itertools, json, math, os, random, time
from datetime import datetime, timedelta, timezone
from common import COUNTRY_OCTETS

WEB_COUNTRIES = {"US": 35, "GB": 10, "DE": 8, "FR": 6, "IN": 12, "BR": 6, "JP": 5,
                 "AU": 4, "CA": 6, "NL": 3, "UA": 1, "NG": 1, "CN": 2, "RU": 1}
UAS = {
    "desktop": ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"],
    "mobile": ["Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
               "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0 Mobile Safari/537.36"],
    "crawler": ["Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"],
    "script": ["python-requests/2.31.0", "curl/8.5.0", "Scrapy/2.11 (+https://scrapy.org)", "Go-http-client/1.1"],
    "scanner": ["sqlmap/1.7.2#stable (https://sqlmap.org)", "Nikto/2.5.0", "zgrab/0.x", "Mozilla/5.0 (compatible; Nmap Scripting Engine)"],
}
PAGES = [("/", 20), ("/products", 12), ("/products/{id}", 18), ("/cart", 6), ("/checkout", 4),
         ("/search?q={q}", 8), ("/api/v1/items", 10), ("/api/v1/orders/{id}", 5),
         ("/static/app.js", 8), ("/static/style.css", 6), ("/images/p{id}.jpg", 10),
         ("/about", 2), ("/blog/{slug}", 4)]
WORDS = ["shoes", "laptop", "coffee", "backpack", "headphones", "lamp", "desk", "mug", "watch", "camera"]
SLUGS = ["how-to-choose-a-laptop", "spring-sale", "our-story", "care-guide", "gift-ideas", "new-arrivals"]
SENSITIVE = ["/.env", "/wp-login.php", "/phpmyadmin/index.php", "/.git/config", "/admin", "/xmlrpc.php",
             "/etc/passwd", "/../../etc/passwd", "/api/v1/users?id=1'+UNION+SELECT+1,2--", "/backup.zip",
             "/server-status", "/wp-admin/setup-config.php", "/config.json", "/.aws/credentials", "/admin/login"]
REFS = ["-", "-", "-", "https://www.google.com/", "https://www.bing.com/", "https://t.co/xyz", "https://news.example.org/"]


class Gen:
    def __init__(self, seed):
        self.r = random.Random(seed)
        r = self.r
        cc = list(WEB_COUNTRIES)
        self.cc_cum = list(itertools.accumulate(WEB_COUNTRIES.values()))
        self.cc = cc
        # visitor pool (zipf-ish popularity)
        self.visitors = []
        for _ in range(6000):
            c = r.choices(cc, cum_weights=self.cc_cum)[0]
            kind = r.choices(["desktop", "mobile", "crawler", "script"], [55, 38, 4, 3])[0]
            self.visitors.append((self.ip(c), r.choice(UAS[kind])))
        self.vis_cum = list(itertools.accumulate(1 / ((i + 1) ** 0.6) for i in range(len(self.visitors))))
        self.page_cum = list(itertools.accumulate(w for _, w in PAGES))
        # users
        self.users = [f"user{i:03d}" for i in range(500)] + ["alice", "bob", "carol", "dave", "erin"]
        self.home = {}
        for u in self.users:
            c = r.choices(cc[:11], cum_weights=list(itertools.accumulate(list(WEB_COUNTRIES.values())[:11])))[0]
            self.home[u] = (c, [self.ip(c) for _ in range(r.randint(1, 3))])

    def ip(self, country):
        o = self.r.choice(COUNTRY_OCTETS[country])
        return f"{o}.{self.r.randint(0, 255)}.{self.r.randint(0, 255)}.{self.r.randint(1, 254)}"

    def fill(self, tpl):
        r = self.r
        return (tpl.replace("{id}", str(r.randint(1, 2000))).replace("{q}", r.choice(WORDS))
                .replace("{slug}", r.choice(SLUGS)))

    def web(self, ts, ip=None, ua=None, path=None, method="GET", status=None, rt=None, nbytes=None):
        r = self.r
        if ip is None:
            ip, ua = r.choices(self.visitors, cum_weights=self.vis_cum)[0]
        if path is None:
            path = self.fill(PAGES[r.choices(range(len(PAGES)), cum_weights=self.page_cum)[0]][0])
        if status is None:
            status = r.choices([200, 304, 301, 404, 500, 403], [93, 3, 1.5, 1.5, 0.4, 0.6])[0]
        if rt is None:
            base = 0.015 if path.startswith(("/static", "/images")) else 0.12 if path.startswith("/api") \
                else 0.25 if path.startswith(("/checkout", "/cart")) else 0.18 if "search" in path else 0.06
            rt = r.lognormvariate(math.log(base), 0.45)
        if nbytes is None:
            if status == 304: nbytes = 0
            elif status >= 400: nbytes = r.randint(300, 900)
            elif path.endswith(".js"): nbytes = r.randint(40000, 120000)
            elif path.endswith(".jpg"): nbytes = r.randint(60000, 300000)
            elif path.startswith("/api"): nbytes = r.randint(400, 8000)
            else: nbytes = r.randint(3000, 40000)
        return dict(ts=ts, ip=ip, ua=ua, path=path, method=method, status=status, rt=rt,
                    bytes=nbytes, ref=r.choice(REFS))

    def auth(self, ts, user, ip, ok, method="password", reason=None, ua=None):
        ev = dict(ts=ts, user=user, ip=ip, result="success" if ok else "failure", method=method,
                  reason=None if ok else (reason or "bad_password"))
        web = self.web(ts, ip=ip, ua=ua or self.r.choice(UAS["desktop"]), path="/login", method="POST",
                       status=302 if ok else 401, rt=self.r.lognormvariate(math.log(0.2), 0.3),
                       nbytes=self.r.randint(200, 600))
        return ev, web

    def normal_auth(self, ts):
        r = self.r
        u = r.choice(self.users)
        c, ips = self.home[u]
        ok = r.random() > 0.045
        return self.auth(ts, u, r.choice(ips), ok, r.choices(["password", "sso", "mfa"], [70, 20, 10])[0],
                         r.choice(["bad_password", "bad_password", "mfa_failed", "locked"]))


def fmt_access(e):
    t = e["ts"].strftime("%d/%b/%Y:%H:%M:%S +0000")
    return (f'{e["ip"]} - - [{t}] "{e["method"]} {e["path"]} HTTP/1.1" {e["status"]} {e["bytes"]} '
            f'"{e["ref"]}" "{e["ua"]}" rt={e["rt"]:.3f}')


def fmt_auth(e):
    return json.dumps({**e, "ts": e["ts"].strftime("%Y-%m-%dT%H:%M:%SZ")})


def diurnal(dt):
    h = dt.hour + dt.minute / 60
    w = 0.3 + 0.7 * (0.5 - 0.5 * math.cos(2 * math.pi * (h - 4) / 24))
    return w * (0.7 if dt.weekday() >= 5 else 1.0)


def sample_times(g, start, end, n):
    hours = int((end - start).total_seconds() // 3600)
    slots = [start + timedelta(hours=i) for i in range(hours)]
    cum = list(itertools.accumulate(diurnal(s) for s in slots))
    return [slots[i] + timedelta(seconds=g.r.random() * 3600) for i in g.r.choices(range(hours), cum_weights=cum, k=n)]


def historical(args):
    g = Gen(args.seed); r = g.r
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=args.days)
    span = (end - start).total_seconds()
    at = lambda frac: start + timedelta(seconds=span * frac)
    web, auth = [], []

    for ts in sample_times(g, start, end, args.requests):
        web.append(g.web(ts))
    for ts in sample_times(g, start, end, args.logins):
        a, w = g.normal_auth(ts); auth.append(a); web.append(w)

    # --- Scenario: DDoS-like spike (20 min) ---
    s0 = at(0.62); bots = [(g.ip(r.choice(list(WEB_COUNTRIES))), r.choice(UAS["script"])) for _ in range(120)]
    for _ in range(int(args.requests * 0.05)):
        ip, ua = r.choice(bots)
        bad = r.random() < 0.3
        web.append(g.web(s0 + timedelta(seconds=r.random() * 1200), ip, ua, r.choice(["/", "/search?q=" + r.choice(WORDS)]),
                         status=503 if bad else 200, rt=r.uniform(1, 4) if bad else r.uniform(0.3, 1.2)))
    # --- Scenario: outage on api/checkout (35 min) ---
    o0, o1 = at(0.40), at(0.40) + timedelta(minutes=35)
    for e in web:
        if o0 <= e["ts"] <= o1 and e["path"].startswith(("/api", "/checkout", "/cart")) and r.random() < 0.6:
            e["status"] = r.choice([500, 502, 503]); e["rt"] *= 5; e["bytes"] = 600
    # --- Scenario: brute force against privileged accounts ---
    for i, (c, target) in enumerate([("RU", "admin"), ("CN", "root"), ("NG", "ceo")]):
        ip = g.ip(c); t0 = at(0.2 + i * 0.22); n = r.randint(90, 220)
        dur = r.randint(2, 4) * 3600
        for _ in range(n):
            a, w = g.auth(t0 + timedelta(seconds=r.random() * dur), target, ip, False, ua=UAS["script"][0])
            auth.append(a); web.append(w)
        if i != 1:  # attacker eventually gets in on two of them
            for k in range(2):
                a, w = g.auth(t0 + timedelta(seconds=dur + 60 * (k + 1)), target, ip, True, ua=UAS["script"][0])
                auth.append(a); web.append(w)
    # --- Scenario: credential stuffing ---
    for i, c in enumerate(["UA", "BR"]):
        ip = g.ip(c); t0 = at(0.55 + i * 0.2)
        for u in r.sample(g.users, 320):
            for _ in range(r.choice([1, 1, 2])):
                a, w = g.auth(t0 + timedelta(seconds=r.random() * 5400), u, ip, r.random() < 0.02, ua=UAS["script"][1])
                auth.append(a); web.append(w)
    # --- Scenario: impossible travel ---
    for u in r.sample(g.users[:500], 7):
        t = at(r.uniform(0.1, 0.9)); c, ips = g.home[u]
        a, w = g.auth(t, u, ips[0], True, "password"); auth.append(a); web.append(w)
        a, w = g.auth(t + timedelta(minutes=r.randint(4, 40)), u, g.ip(r.choice(["RU", "CN", "NG"])), True, "password")
        auth.append(a); web.append(w)
    # --- Scenario: vulnerability scanners ---
    for i in range(5):
        ip = g.ip(r.choice(["RU", "CN", "NG", "UA", "BR"])); ua = r.choice(UAS["scanner"] + UAS["script"])
        t0 = at(r.uniform(0.05, 0.95)); dur = r.randint(1200, 3600)
        for _ in range(r.randint(150, 500)):
            p = r.choice(SENSITIVE) if r.random() < 0.6 else f"/{r.choice(WORDS)}{r.randint(1,999)}.php"
            web.append(g.web(t0 + timedelta(seconds=r.random() * dur), ip, ua, p,
                             status=r.choices([404, 403, 400, 200], [80, 12, 6, 2])[0], rt=r.uniform(0.01, 0.08)))
    # --- Scenario: content scrapers ---
    for ip, ua, c in [(g.ip("NL"), UAS["script"][2], "NL"), (g.ip("IN"), UAS["script"][0], "IN")]:
        t0 = at(r.uniform(0.1, 0.7))
        for _ in range(r.randint(3000, 6000)):
            web.append(g.web(t0 + timedelta(seconds=r.random() * 3 * 3600), ip, ua, f"/products/{r.randint(1, 2000)}",
                             status=r.choices([200, 429], [85, 15])[0]))

    web.sort(key=lambda e: e["ts"]); auth.sort(key=lambda e: e["ts"])
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "access.log"), "w") as f:
        for e in web:
            f.write(fmt_access(e) + "\n")
            if r.random() < 0.0003:
                f.write("### corrupted line " + str(r.randint(0, 10**6)) + "\n")
    with open(os.path.join(args.out, "auth.log"), "w") as f:
        for e in auth:
            f.write(fmt_auth(e) + "\n")
    print(f"Wrote {len(web):,} access lines and {len(auth):,} auth events to {args.out}/")
    print(f"Time span: {start:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M} UTC")


def live(args):
    g = Gen(args.seed + 1); r = g.r
    os.makedirs(args.out, exist_ok=True)
    print("Live mode: appending events. Ctrl+C to stop.")
    with open(os.path.join(args.out, "access.log"), "a") as fa, open(os.path.join(args.out, "auth.log"), "a") as fu:
        while True:
            now = datetime.now(timezone.utc)
            for _ in range(r.randint(3, 25)):
                fa.write(fmt_access(g.web(now)) + "\n")
            for _ in range(r.randint(0, 3)):
                a, w = g.normal_auth(now); fu.write(fmt_auth(a) + "\n"); fa.write(fmt_access(w) + "\n")
            if r.random() < 0.08:  # occasional brute-force burst
                ip, tgt = g.ip(r.choice(["RU", "CN", "NG"])), r.choice(["admin", "root"])
                for _ in range(r.randint(15, 40)):
                    a, w = g.auth(now, tgt, ip, False, ua=UAS["script"][0]); fu.write(fmt_auth(a) + "\n"); fa.write(fmt_access(w) + "\n")
                print(f"  burst: brute force from {ip} on {tgt}")
            fa.flush(); fu.flush()
            time.sleep(args.interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--requests", type=int, default=250_000)
    ap.add_argument("--logins", type=int, default=30_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="logs")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--interval", type=float, default=2.0)
    a = ap.parse_args()
    live(a) if a.live else historical(a)
